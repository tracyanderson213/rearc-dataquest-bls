#!/usr/bin/env python3
"""
Mirror BLS time series directory into S3 with polite crawling, retries, and idempotency.
FIXED: Now complies with BLS data access policies with proper User-Agent containing contact info.

Environment variables (all optional; sensible defaults provided):
  BLS_BASE_URL       default: https://download.bls.gov/pub/time.series/
  BLS_S3_BUCKET      default: dataquest-gov-bls-timeseries
  BLS_S3_PREFIX      default: bls
  AWS_REGION         default: us-east-2
  BLS_USER_AGENT     default: Compliant bot with contact info (see code)
  BLS_CONTACT        default: tracy.anderson@outlook.com
  BLS_RATE_LIMIT_RPS default: 1
  BLS_HTTP_TIMEOUT   default: 60
  LOG_LEVEL          default: INFO

CLI:
  python sync_bls.py                 # full sync
  python sync_bls.py --dry-run       # print actions, don't modify S3
  python sync_bls.py --test <url>    # fetch a single URL to validate access (writes error dump on failure)
"""

import argparse
import itertools
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

# -----------------------------
# Config (env overrides allowed)
# -----------------------------
BASE_URL = os.environ.get("BLS_BASE_URL", "https://download.bls.gov/pub/time.series/")
BUCKET   = os.environ.get("BLS_S3_BUCKET", "dataquest-gov-bls-timeseries")
PREFIX   = os.environ.get("BLS_S3_PREFIX", "bls")
REGION   = os.environ.get("AWS_REGION", "us-east-2")

# BLS-compliant contact information
CONTACT = os.environ.get("BLS_CONTACT", "tracy.anderson@outlook.com")

# BLS-compliant User-Agent with contact information as required by policy
# This is the key fix for 403 errors per BLS policy at https://www.bls.gov/bls/pss.htm
USER_AGENT = os.environ.get(
    "BLS_USER_AGENT",
    f"BLS-Data-Sync/1.0 (Educational/Research Purpose; Contact: {CONTACT}; "
    f"Project: github.com/tracyanderson213/rearc-dataquest-bls; "
    f"Compliance: https://www.bls.gov/bls/pss.htm)"
)

RPS = float(os.environ.get("BLS_RATE_LIMIT_RPS", "1"))  # Conservative 1 RPS
SLEEP = 1.0 / max(RPS, 0.1)
TIMEOUT = int(os.environ.get("BLS_HTTP_TIMEOUT", "60"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bls-sync")

# -----------------------------
# HTTP setup with BLS-compliant headers
# -----------------------------
session = requests.Session()

# Headers that comply with BLS policy - include contact info
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "From": CONTACT,  # Additional contact header
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    # Compliance statement
    "X-Purpose": "Educational research data synchronization",
    "X-Compliance": "https://www.bls.gov/bls/pss.htm"
}

session.headers.update(COMMON_HEADERS)

# Configure adapter with conservative settings
adapter = requests.adapters.HTTPAdapter(
    pool_connections=1,  # Single connection to be polite
    pool_maxsize=2,
    max_retries=0
)
session.mount("http://", adapter)
session.mount("https://", adapter)

log.info("BLS-compliant User-Agent: %s", USER_AGENT)
log.info("Contact: %s", CONTACT)

# -----------------------------
# AWS S3 client
# -----------------------------
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 10, "mode": "standard"}))

# -----------------------------
# Helpers
# -----------------------------
def backoff(retries: int) -> float:
    """Conservative exponential backoff to be polite."""
    base = min(300.0, 2 ** retries)  # Cap at 5 minutes
    return base + random.uniform(0.0, 5.0)  # Large jitter

def save_error_response(url: str, resp: requests.Response) -> None:
    """Dump response to a timestamped file for debugging."""
    try:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"bls_error_{resp.status_code}_{ts}.txt"
        with open(fname, "w", encoding="utf-8", errors="replace") as f:
            f.write(f"URL: {url}\n")
            f.write(f"Final URL: {getattr(resp, 'url', url)}\n")
            f.write(f"Status: {resp.status_code}\n")
            f.write(f"Reason: {getattr(resp, 'reason', 'Unknown')}\n\n")
            f.write("=== Request Headers ===\n")
            if hasattr(resp.request, 'headers'):
                for k, v in resp.request.headers.items():
                    f.write(f"{k}: {v}\n")
            f.write("\n=== Response Headers ===\n")
            for k, v in resp.headers.items():
                f.write(f"{k}: {v}\n")
            f.write("\n=== Body (first 10k chars) ===\n")
            f.write(resp.text[:10_000])
        log.warning("Saved error response to %s", fname)
    except Exception as e:
        log.warning("Could not write error dump: %s", e)

def get_url(url: str, allow_redirects: bool = True) -> requests.Response:
    """Polite GET request with BLS-compliant headers."""
    retries = 0
    last_resp = None
    
    while True:
        try:
            # Conservative delay with jitter
            sleep_time = SLEEP + random.uniform(1.0, 3.0)  # Extra politeness
            time.sleep(sleep_time)
            
            log.debug(f"GET attempt {retries + 1} for {url}")
            
            resp = session.get(
                url, 
                headers=COMMON_HEADERS, 
                timeout=TIMEOUT, 
                allow_redirects=allow_redirects
            )
            last_resp = resp
            
            log.debug("GET %s -> %s (%s)", url, resp.status_code, 
                     getattr(resp, 'reason', 'Unknown'))
            
            if resp.status_code == 403:
                # Still getting 403 - log details and retry with longer delay
                log.warning("403 Forbidden for %s (attempt %d/7)", url, retries + 1)
                save_error_response(url, resp)
                if retries < 6:
                    raise requests.HTTPError(f"403 Forbidden - retrying with longer delay")
                else:
                    # Final attempt failed
                    log.error("Persistent 403 error. Check User-Agent compliance with BLS policy")
                    log.error("Current User-Agent: %s", USER_AGENT)
                    log.error("BLS Policy: https://www.bls.gov/bls/pss.htm")
                    resp.raise_for_status()
                    
            elif resp.status_code in (429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
                
            resp.raise_for_status()
            return resp
            
        except Exception as e:
            if retries >= 6:
                log.error("Failed to GET %s after %d retries: %s", url, retries + 1, e)
                if last_resp is not None and not getattr(last_resp, "ok", True):
                    save_error_response(url, last_resp)
                raise
                
            wait = backoff(retries)
            log.warning("GET %s failed (%s). retrying in %.1fs (attempt %d/7)", 
                       url, e, wait, retries + 1)
            time.sleep(wait)
            retries += 1

def head_url(url: str) -> requests.Response:
    """Polite HEAD request with fallback to GET."""
    retries = 0
    last_resp = None
    
    while True:
        try:
            sleep_time = SLEEP + random.uniform(1.0, 3.0)
            time.sleep(sleep_time)
            
            resp = session.head(url, headers=COMMON_HEADERS, timeout=TIMEOUT, allow_redirects=True)
            last_resp = resp
            
            if resp.status_code == 405:
                log.debug("HEAD not allowed for %s, falling back to GET", url)
                return get_url(url)
                
            if resp.status_code == 403:
                log.warning("403 Forbidden on HEAD for %s, trying GET instead", url)
                return get_url(url)
                
            elif resp.status_code in (429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
                
            resp.raise_for_status()
            return resp
            
        except Exception as e:
            if retries >= 6:
                log.warning("Failed to HEAD %s after %d retries, falling back to GET", url, retries + 1)
                try:
                    return get_url(url)
                except:
                    if last_resp is not None and not getattr(last_resp, "ok", True):
                        save_error_response(url, last_resp)
                    raise
                    
            wait = backoff(retries)
            log.warning("HEAD %s failed (%s). retrying in %.1fs", url, e, wait)
            time.sleep(wait)
            retries += 1

# -----------------------------
# Parse directory listings (unchanged from original)
# -----------------------------
HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)

def list_subdirs(base_url: str) -> List[str]:
    """Return list of subdirectory URLs under base_url (e.g., pc/, pr/, sm/)."""
    log.info("Discovering subdirectories under %s", base_url)
    
    try:
        resp = get_url(base_url)
        log.debug("Directory listing response length: %d chars", len(resp.text))
        
        hrefs = HREF_RE.findall(resp.text)
        log.debug("Found %d href attributes", len(hrefs))
        
        subs = []
        for h in hrefs:
            if h in ("../", "./", "/"):
                continue
            if h.endswith("/") and not h.startswith(("http://", "https://", "mailto:")):
                full_url = urljoin(base_url, h)
                subs.append(full_url)
                log.debug("Found subdir: %s", full_url)
                
        log.info("Discovered %d subdirectories", len(subs))
        return subs
        
    except Exception as e:
        log.error("Failed to list subdirectories from %s: %s", base_url, e)
        raise

def list_files(dir_url: str) -> List[Tuple[str, str]]:
    """Return (filename, file_url) tuples for a directory URL."""
    log.info("Listing files in %s", dir_url)
    
    try:
        resp = get_url(dir_url)
        hrefs = HREF_RE.findall(resp.text)
        
        files = []
        for h in hrefs:
            if not h or h.endswith("/") or h.startswith(("http://", "https://", "mailto:")):
                continue
            if h not in ("../", "./"):
                full_url = urljoin(dir_url, h)
                files.append((h, full_url))
                
        log.debug("Found %d files in %s", len(files), dir_url)
        return files
        
    except Exception as e:
        log.error("Failed to list files from %s: %s", dir_url, e)
        return []

# -----------------------------
# S3 helpers (unchanged from original)
# -----------------------------
def s3_key_for(url: str, base_url: str, prefix: str) -> str:
    """Map a source URL to an S3 key under the given prefix."""
    path = urlparse(url).path
    base_path = urlparse(base_url).path
    rel = path[len(base_path):].lstrip("/")
    return f"{prefix}/{rel}"

def get_s3_object_head(bucket: str, key: str) -> Dict:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NotFound"):
            return {}
        raise

def upload_if_changed(bucket: str, key: str, src_url: str, src_len: int, src_mtime_http: str, dry_run: bool=False) -> bool:
    """Upload only if changed. Compare source Last-Modified + Content-Length to S3."""
    head = get_s3_object_head(bucket, key)
    existing_len = int(head.get("ContentLength", -1)) if head else -1
    existing_mtime = (head.get("Metadata", {}) or {}).get("src-last-modified", "")

    if existing_len == src_len and existing_mtime == src_mtime_http:
        log.info("SKIP unchanged %s", key)
        return False

    resp = get_url(src_url)
    data = resp.content
    if src_len >= 0 and len(data) != src_len:
        log.warning("Length mismatch for %s (HEAD=%s, body=%s). Proceeding.", src_url, src_len, len(data))

    if dry_run:
        log.info("DRY-RUN: would PUT %s (%d bytes)", key, len(data))
        return True

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=BytesIO(data),
        ServerSideEncryption="AES256",
        Metadata={
            "src-last-modified": src_mtime_http or "",
            "src-content-length": str(src_len),
            "src-url": src_url
        },
        ContentType="text/plain" if key.endswith((".txt", ".series", ".contacts", ".footnote")) else "application/octet-stream"
    )
    log.info("PUT  %s  (%d bytes)", key, len(data))
    return True

def list_s3_keys(bucket: str, prefix: str) -> List[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def delete_keys(bucket: str, keys: List[str], dry_run: bool=False) -> None:
    if not keys:
        return
    if dry_run:
        log.info("DRY-RUN: would DELETE %d objects", len(keys))
        return
    batch = []
    for k in keys:
        batch.append({"Key": k})
        if len(batch) == 1000:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            log.info("DELETE %d objects", len(batch))
            batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        log.info("DELETE %d objects", len(batch))

# -----------------------------
# Main sync with compliance check
# -----------------------------
def run_sync(dry_run: bool=False) -> None:
    log.info("Starting BLS-compliant sync: %s -> s3://%s/%s/", BASE_URL, BUCKET, PREFIX)
    log.info("AWS region: %s  Bucket: %s  Prefix: %s  RPS: %s", REGION, BUCKET, PREFIX, RPS)
    log.info("BLS compliance: User-Agent includes contact info per https://www.bls.gov/bls/pss.htm")

    # Test connection with compliance headers
    log.info("Testing BLS compliance with proper headers...")
    try:
        test_resp = head_url(BASE_URL)
        log.info("✓ Connection successful (status: %s) - BLS accepts our headers!", test_resp.status_code)
    except Exception as e:
        log.error("✗ Connection failed: %s", e)
        log.error("This may indicate:")
        log.error("1. User-Agent still not compliant with BLS policy")
        log.error("2. IP-based blocking")
        log.error("3. Network/DNS issues")
        log.error("Current User-Agent: %s", USER_AGENT)
        sys.exit(1)

    time.sleep(3.0)  # Extra politeness delay

    # Discovery and sync (same as original)
    try:
        subdirs = list_subdirs(BASE_URL)
        if not subdirs:
            log.error("No subdirectories discovered under %s", BASE_URL)
            sys.exit(2)
    except Exception as e:
        log.error("Failed to discover subdirectories: %s", e)
        sys.exit(1)

    source_map: Dict[str, Tuple[str, int, str]] = {}
    failed_dirs = []

    for d in sorted(subdirs):
        try:
            files = list_files(d)
            log.info("Found %d files in %s", len(files), d)
            
            for name, url in files:
                try:
                    h = head_url(url)
                    src_mtime_http = h.headers.get("Last-Modified", "")
                    src_len = int(h.headers.get("Content-Length", "-1"))
                    key = s3_key_for(url, BASE_URL, PREFIX)
                    source_map[key] = (url, src_len, src_mtime_http)
                except Exception as e:
                    log.warning("Failed to get metadata for %s: %s", url, e)
                    continue
                    
        except Exception as e:
            log.error("Failed to process directory %s: %s", d, e)
            failed_dirs.append(d)
            continue

    if not source_map:
        log.error("No files discovered")
        sys.exit(1)

    # Upload and cleanup (same as original)
    uploaded = skipped = failed = 0
    for key, (url, src_len, src_mtime_http) in sorted(source_map.items()):
        try:
            changed_now = upload_if_changed(BUCKET, key, url, src_len, src_mtime_http, dry_run=dry_run)
            if changed_now:
                uploaded += 1
            else:
                skipped += 1
        except Exception as e:
            log.error("Failed to sync %s -> s3://%s/%s : %s", url, BUCKET, key, e)
            failed += 1
            continue

    try:
        s3_keys_now = set(list_s3_keys(BUCKET, PREFIX))
        source_keys = set(source_map.keys())
        orphans = sorted(s3_keys_now - source_keys)
        delete_keys(BUCKET, orphans, dry_run=dry_run)
    except Exception as e:
        log.error("Failed to clean up orphaned files: %s", e)

    log.info("Sync complete. uploaded=%d skipped=%d failed=%d deleted=%d total=%d dry_run=%s",
             uploaded, skipped, failed, len(orphans) if 'orphans' in locals() else 0, len(source_map), dry_run)

# -----------------------------
# Enhanced test mode
# -----------------------------
def test_one(url: str) -> None:
    log.info("Testing BLS compliance for URL: %s", url)
    log.info("User-Agent: %s", USER_AGENT)
    log.info("Contact: %s", CONTACT)
    
    try:
        log.info("Testing HEAD request...")
        r = head_url(url)
        log.info("✓ HEAD OK %s -> status %s", url, r.status_code)
        
        log.info("Testing GET request...")
        r = get_url(url)
        log.info("✓ GET OK %s -> %s bytes", url, len(r.content))
        log.info("✓ BLS accepts our compliance headers!")
        
        if len(r.text) < 1000:
            log.info("Response preview:\n%s", r.text[:500])
        else:
            log.info("Response preview (first 500 chars):\n%s", r.text[:500])
            
    except Exception as e:
        log.error("✗ Test failed: %s", e)
        log.error("Verify User-Agent compliance: https://www.bls.gov/bls/pss.htm")
        sys.exit(1)

# -----------------------------
# Entrypoint
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="BLS-compliant data sync to S3")
    p.add_argument("--dry-run", action="store_true", help="Print actions; don't modify S3")
    p.add_argument("--test", metavar="URL", help="Test BLS compliance for a single URL")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.test:
        test_one(args.test)
    else:
        run_sync(dry_run=args.dry_run)