#!/usr/bin/env python3
# Save this file as: bls_api_sync.py
"""
Mirror BLS data using the official BLS API instead of web scraping.

This avoids 403 errors and is the official way to access BLS data programmatically.
Requires BLS API key for higher rate limits (optional).

Environment variables:
  BLS_API_KEY        optional: your BLS API registration key
  BLS_S3_BUCKET      default: dataquest-gov-bls-timeseries
  BLS_S3_PREFIX      default: bls-api
  AWS_REGION         default: us-east-2
  LOG_LEVEL          default: INFO

CLI:
  python bls_api_sync.py --dry-run       # test without uploading
  python bls_api_sync.py --series-list   # show available series
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, List, Optional

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

# Configuration
API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
API_KEY = os.environ.get("BLS_API_KEY")  # Optional but recommended
BUCKET = os.environ.get("BLS_S3_BUCKET", "dataquest-gov-bls-timeseries")
PREFIX = os.environ.get("BLS_S3_PREFIX", "bls-api")
REGION = os.environ.get("AWS_REGION", "us-east-2")

# Rate limits based on API key
if API_KEY:
    DAILY_LIMIT = 500
    SERIES_PER_REQUEST = 50
    YEARS_PER_REQUEST = 20
    REQUEST_DELAY = 0.1  # 10 requests per second
else:
    DAILY_LIMIT = 25
    SERIES_PER_REQUEST = 25
    YEARS_PER_REQUEST = 10
    REQUEST_DELAY = 1.0  # 1 request per second

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bls-api-sync")

# AWS S3 client
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 10, "mode": "standard"}))

# Popular BLS series that are commonly requested
COMMON_SERIES = {
    "employment": [
        "LNS14000000",  # Unemployment Rate
        "LNS11300000",  # Labor Force Participation Rate
        "CES0000000001",  # Total Nonfarm Employment
        "LNS12000000",  # Employment Level
    ],
    "prices": [
        "CUUR0000SA0",  # Consumer Price Index - All Urban Consumers
        "CUUR0000SA0L1E",  # CPI - All items less food and energy
        "CUSR0000SA0",  # CPI - All Urban Consumers (seasonally adjusted)
    ],
    "wages": [
        "CES0500000003",  # Average Hourly Earnings - Total Private
        "LES1252881600Q",  # Employment Cost Index
    ],
    "productivity": [
        "PRS85006092",  # Nonfarm Business Sector: Labor Productivity
    ]
}

def bls_api_request(series_ids: List[str], start_year: int, end_year: int) -> Dict:
    """Make a request to BLS API for given series and date range."""
    
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year)
    }
    
    if API_KEY:
        payload["registrationkey"] = API_KEY
    
    log.debug("API request: %d series, %d-%d", len(series_ids), start_year, end_year)
    
    try:
        response = requests.post(
            API_BASE_URL,
            json=payload,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") != "REQUEST_SUCCEEDED":
            log.error("BLS API error: %s", data.get("message", "Unknown error"))
            return {}
            
        return data
        
    except Exception as e:
        log.error("API request failed: %s", e)
        return {}

def get_s3_object_head(bucket: str, key: str) -> Dict:
    """Get S3 object metadata, return empty dict if not found."""
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NotFound"):
            return {}
        raise

def upload_series_data(bucket: str, series_id: str, data: Dict, dry_run: bool = False) -> bool:
    """Upload series data to S3 if changed."""
    
    # Create S3 key
    timestamp = datetime.now().strftime("%Y%m%d")
    key = f"{PREFIX}/{series_id}/{series_id}_{timestamp}.json"
    
    # Convert to JSON
    json_data = json.dumps(data, indent=2)
    data_bytes = json_data.encode('utf-8')
    
    # Check if already exists and unchanged
    head = get_s3_object_head(bucket, key)
    if head:
        existing_len = head.get("ContentLength", 0)
        if existing_len == len(data_bytes):
            log.info("SKIP unchanged %s", key)
            return False
    
    if dry_run:
        log.info("DRY-RUN: would PUT %s (%d bytes)", key, len(data_bytes))
        return True
    
    # Upload
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=BytesIO(data_bytes),
        ServerSideEncryption="AES256",
        ContentType="application/json",
        Metadata={
            "series-id": series_id,
            "sync-timestamp": datetime.utcnow().isoformat(),
            "source": "bls-api-v2"
        }
    )
    
    log.info("PUT %s (%d bytes)", key, len(data_bytes))
    return True

def sync_series_group(series_ids: List[str], start_year: int, end_year: int, dry_run: bool = False) -> int:
    """Sync a group of series for given date range."""
    
    uploaded = 0
    
    # Split into API-friendly chunks
    for i in range(0, len(series_ids), SERIES_PER_REQUEST):
        chunk = series_ids[i:i + SERIES_PER_REQUEST]
        
        # Split years into chunks if needed
        current_year = start_year
        while current_year <= end_year:
            year_end = min(current_year + YEARS_PER_REQUEST - 1, end_year)
            
            log.info("Fetching %d series for %d-%d", len(chunk), current_year, year_end)
            
            data = bls_api_request(chunk, current_year, year_end)
            
            if not data or "Results" not in data:
                log.warning("No data returned for %s (%d-%d)", chunk, current_year, year_end)
                current_year = year_end + 1
                continue
            
            # Process each series
            for series_data in data["Results"]["series"]:
                series_id = series_data["seriesID"]
                
                # Create enhanced data structure
                enhanced_data = {
                    "seriesID": series_id,
                    "data": series_data.get("data", []),
                    "catalog": series_data.get("catalog", {}),
                    "api_metadata": {
                        "request_time": datetime.utcnow().isoformat(),
                        "start_year": current_year,
                        "end_year": year_end,
                        "api_version": "v2"
                    }
                }
                
                try:
                    if upload_series_data(BUCKET, series_id, enhanced_data, dry_run):
                        uploaded += 1
                except Exception as e:
                    log.error("Failed to upload %s: %s", series_id, e)
            
            current_year = year_end + 1
            time.sleep(REQUEST_DELAY)
    
    return uploaded

def run_sync(dry_run: bool = False, series_filter: Optional[str] = None) -> None:
    """Main sync function."""
    
    log.info("Starting BLS API sync -> s3://%s/%s/", BUCKET, PREFIX)
    log.info("API key: %s", "configured" if API_KEY else "not configured (25 requests/day limit)")
    log.info("Rate limits: %d series/req, %d years/req, %.1fs delay", 
             SERIES_PER_REQUEST, YEARS_PER_REQUEST, REQUEST_DELAY)
    
    # Calculate date range (last 10 years)
    current_year = datetime.now().year
    start_year = current_year - 10
    
    total_uploaded = 0
    
    # Process series groups
    for category, series_list in COMMON_SERIES.items():
        if series_filter and series_filter not in category:
            log.info("Skipping %s (filter: %s)", category, series_filter)
            continue
            
        log.info("Processing %s series (%d series)", category, len(series_list))
        
        try:
            uploaded = sync_series_group(series_list, start_year, current_year, dry_run)
            total_uploaded += uploaded
            log.info("Completed %s: %d files uploaded", category, uploaded)
            
        except Exception as e:
            log.error("Failed to process %s: %s", category, e)
            continue
    
    log.info("Sync complete: %d total files uploaded, dry_run=%s", total_uploaded, dry_run)

def list_available_series() -> None:
    """Show available series that can be synced."""
    print("\nAvailable BLS series for sync:\n")
    
    for category, series_list in COMMON_SERIES.items():
        print(f"{category.upper()}:")
        for series_id in series_list:
            print(f"  {series_id}")
        print()
    
    print(f"Total: {sum(len(series) for series in COMMON_SERIES.values())} series")
    print("\nTo add more series, edit the COMMON_SERIES dictionary in the script.")
    print("Find series IDs at: https://www.bls.gov/data/")

def test_api_access() -> None:
    """Test BLS API access with a simple request."""
    log.info("Testing BLS API access...")
    
    test_series = ["LNS14000000"]  # Unemployment rate
    current_year = datetime.now().year
    
    data = bls_api_request(test_series, current_year - 1, current_year)
    
    if data and "Results" in data:
        series_data = data["Results"]["series"][0]
        data_points = len(series_data.get("data", []))
        log.info("✓ API test successful: %d data points for %s", data_points, test_series[0])
        
        if data_points > 0:
            latest = series_data["data"][0]
            log.info("Latest data: %s %s = %s", latest.get("year"), latest.get("period"), latest.get("value"))
    else:
        log.error("✗ API test failed")
        return False
    
    return True

def parse_args():
    parser = argparse.ArgumentParser(description="Sync BLS data via official API")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without uploading")
    parser.add_argument("--test", action="store_true", help="Test API access only")
    parser.add_argument("--series-list", action="store_true", help="List available series")
    parser.add_argument("--category", help="Sync only specific category (employment, prices, wages, productivity)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    if args.series_list:
        list_available_series()
    elif args.test:
        test_api_access()
    else:
        run_sync(dry_run=args.dry_run, series_filter=args.category)