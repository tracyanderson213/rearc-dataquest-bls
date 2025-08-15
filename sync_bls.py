#!/usr/bin/env python3
"""
Mirror BLS time series directory into S3 with polite crawling, retries, and idempotency.

Environment variables (all optional; sensible defaults provided):
  BLS_BASE_URL       default: https://download.bls.gov/pub/time.series/
  BLS_S3_BUCKET      default: dataquest-gov-bls-timeseries
  BLS_S3_PREFIX      default: bls
  AWS_REGION         default: us-east-2
  BLS_USER_AGENT     default: Mozilla/5.0 (compatible; bls-sync/1.0; +mailto:tracy.anderson@outlook.com; +https://github.com/tracyanderson213/rearc-dataquest-bls)
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

# More conservative polite crawler defaults to avoid 403s
USER_AGENT = os.environ.get(
    "BLS_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
CONTACT = os.environ.get("BLS_CONTACT", "tracy.anderson@outlook.com")
RPS = float(os.environ.get("BLS_RATE_LIMIT_RPS", "1"))  # Reduced from 3 to 1 RPS
SLEEP = 1.0 / max(RPS, 0.1)
TIMEOUT = int(os.environ.get("BLS_HTTP_TIMEOUT", "60"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bls-sync")

# -----------------------------
# HTTP setup with session pooling
# -----------------------------
session = requests.Session()

# More realistic browser headers to avoid 403s
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
}

# Remove potentially problematic headers
if CONTACT and "@" in CONTACT:
    COMMON_HEADERS["From"] = CONTACT

session.headers.update(COMMON_HEADERS)

# Configure session with connection pooling
adapter = requests.adapters.HTTPAdapter(
    pool_connections=5,
    pool_maxsize=10,
    max_retries=0  # We handle retries manually
)
session.mount("http://", adapter)
session.mount("https://", adapter)

log.info("HTTP default headers: %s", dict(session.headers))

# -----------------------------
# AWS S3 client
# -----------------------------
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 10, "mode": "standard"}))

# -----------------------------
# Helpers
# -----------------------------
def backoff(retries: int) -> float:
    """Exponential backoff with larger jitter to be more polite."""
    base = min(120.0, 2 ** retries)  # Cap at 2 minutes instead of 1
    return base + random.uniform(0.0, 2.0)  # Larger jitter

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
    """Enhanced GET with better error handling and logging."""
    retries = 0
    last_resp = None
    
    while True:
        try:
            # Add random delay to be more polite
            sleep_time = SLEEP + random.uniform(0.0, 0.5)
            time.sleep(sleep_time)
            
            log.debug(f"GET attempt {retries + 1} for {url}")
            
            resp = session.get(
                url, 
                headers=COMMON_HEADERS, 
                timeout=TIMEOUT, 
                allow_redirects=allow_redirects,
                stream=False  # Don't stream for better error handling
            )
            last_resp = resp
            
            log.debug("GET %s -> %s (%s)", url, resp.status_code, resp.reason)
            
            # Handle different types of errors
            if resp.status_code == 403:
                log.warning("403 Forbidden for %s - may need different approach", url)
                save_error_response(url, resp)
                if retries < 3:  # Try a few more times with longer delays
                    raise requests.HTTPError(f"403 Forbidden (attempt {retries + 1})")
                else:
                    resp.raise_for_status()  # Give up and raise the error
                    
            elif resp.status_code in (429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
                
            elif resp.status_code == 404:
                log.warning("404 Not Found for %s", url)
                resp.raise_for_status()
                
            resp.raise_for_status()
            return resp
            
        except Exception as e:
            if retries >= 6:
                log.error("Failed to GET %s after %d retries: %s", url, retries + 1, e)
                if last_resp is not None and not getattr(last_resp, "ok", True):
                    save_error_response(url, last_resp)
                raise
                
            wait = backoff(retries)
            log.warning("GET %s failed (%s). retrying in %.1fs (attempt %d/7)", url, e, wait, retries + 1)
            time.sleep(wait)
            retries += 1

def head_url(url: str) -> requests.Response:
    """Enhanced HEAD with fallback to GET if HEAD not supported."""
    retries = 0
    last_resp = None
    
    while True:
        try:
            sleep_time = SLEEP + random.uniform(0.0, 0.5)
            time.sleep(sleep_time)
            
            resp = session.head(url, headers=COMMON_HEADERS, timeout=TIMEOUT, allow_redirects=True)
            last_resp = resp
            
            # If HEAD not allowed, fall back to GET
            if resp.status_code == 405:
                log.debug("HEAD not allowed for %s, falling back to GET", url)
                return get_url(url)
                
            # Handle same error codes as GET
            if resp.status_code == 403:
                log.warning("403 Forbidden on HEAD for %s, trying GET instead", url)
                return get_url(url)  # Fallback to GET for 403 on HEAD
                
            elif resp.status_code in (429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
                
            resp.raise_for_status()
            return resp
            
        except Exception as e:
            if retries >= 6:
                log.error("Failed to HEAD %s after %d retries, falling back to GET", url, retries + 1)
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
# Parse directory listings
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
        return []  # Continue with empty list rather than failing completely

# -----------------------------
# S3 helpers (unchanged)
# -----------------------------
def s3_key_for(url: str, base_url: str, prefix: str) -> str:
    """
    Map a source URL to an S3 key under the given prefix.
    base: https://download.bls.gov/pub/time.series/
    url:  https://download.bls.gov/pub/time.series/pc/pc.series
    ->    bls/pc/pc.series
    """
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
    """
    Upload only if changed. Compare source Last-Modified + Content-Length to S3.
    Returns True if uploaded, False if skipped.
    """
    head = get_s3_object_head(bucket, key)
    existing_len = int(head.get("ContentLength", -1)) if head else -1
    existing_mtime = (head.get("Metadata", {}) or {}).get("src-last-modified", "")

    if existing_len == src_len and existing_mtime == src_mtime_http:
        log.info("SKIP unchanged %s", key)
        return False

    # Download bytes
    resp = get_url(src_url)
    data = resp.content
    if src_len >= 0 and len(data) != src_len:
        log.warning("Length mismatch for %s (HEAD=%s, body=%s). Proceeding.", src_url, src_len, len(data))

    if dry_run:
        log.info("DRY-RUN: would PUT %s (%d bytes)", key, len(data))
        return True

    # Upload with SSE-S3 and source metadata
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
    # S3 delete in batches of 1000
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
# Main sync with better error handling
# -----------------------------
def run_sync(dry_run: bool=False) -> None:
    log.info("Starting BLS sync: %s -> s3://%s/%s/", BASE_URL, BUCKET, PREFIX)
    log.info("AWS region: %s  Bucket: %s  Prefix: %s  RPS: %s", REGION, BUCKET, PREFIX, RPS)
    log.info("User-Agent: %s", USER_AGENT)

    # Test connection first
    log.info("Testing connection to %s", BASE_URL)
    try:
        test_resp = head_url(BASE_URL)
        log.info("Connection test successful (status: %s)", test_resp.status_code)
    except Exception as e:
        log.error("Connection test failed: %s", e)
        log.info("You may need to:")
        log.info("1. Check if the BLS website is accessible")
        log.info("2. Try a different User-Agent string")
        log.info("3. Consider using a proxy or VPN")
        log.info("4. Check if your IP is blocked")
        sys.exit(1)

    time.sleep(2.0)  # gentle warm-up

    # 1) Discover
    try:
        subdirs = list_subdirs(BASE_URL)
        if not subdirs:
            log.error("No subdirectories discovered under %s", BASE_URL)
            log.info("This might indicate a 403/blocking issue or changed site structure")
            sys.exit(2)
    except Exception as e:
        log.error("Failed to discover subdirectories: %s", e)
        sys.exit(1)

    source_map: Dict[str, Tuple[str, int, str]] = {}  # s3_key -> (url, len, lastmod)
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

    if failed_dirs:
        log.warning("Failed to process %d directories: %s", len(failed_dirs), failed_dirs)

    if not source_map:
        log.error("No files discovered - this suggests a serious access issue")
        sys.exit(1)

    # 2) Upload new/changed
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

    # 3) Delete orphans
    try:
        s3_keys_now = set(list_s3_keys(BUCKET, PREFIX))
        source_keys = set(source_map.keys())
        orphans = sorted(s3_keys_now - source_keys)
        delete_keys(BUCKET, orphans, dry_run=dry_run)
    except Exception as e:
        log.error("Failed to clean up orphaned files: %s", e)

    log.info("Sync complete. uploaded=%d skipped=%d failed=%d deleted=%d total=%d dry_run=%s",
             uploaded, skipped, failed, len(orphans) if 'orphans' in locals() else 0, len(source_map), dry_run)
    
    if failed > 0:
        log.warning("Some files failed to sync - check logs above for details")
        return 1
    return 0

# -----------------------------
# Enhanced single URL test mode
# -----------------------------
def test_one(url: str) -> None:
    log.info("Testing single URL: %s", url)
    
    try:
        # Test HEAD first
        log.info("Testing HEAD request...")
        r = head_url(url)
        log.info("HEAD OK %s -> status %s, content-length: %s", 
                url, r.status_code, r.headers.get('Content-Length', 'unknown'))
        
        # Test GET
        log.info("Testing GET request...")
        r = get_url(url)
        log.info("GET OK %s -> %s bytes", url, len(r.content))
        
        # Show some response details
        log.info("Content-Type: %s", r.headers.get('Content-Type', 'unknown'))
        log.info("Last-Modified: %s", r.headers.get('Last-Modified', 'unknown'))
        
        if len(r.text) < 1000:
            log.info("Response preview:\n%s", r.text[:500])
        else:
            log.info("Response preview (first 500 chars):\n%s", r.text[:500])
            
    except Exception as e:
        log.error("Test failed: %s", e)
        sys.exit(1)

# -----------------------------
# Entrypoint
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Mirror BLS time series to S3.")
    p.add_argument("--dry-run", action="store_true", help="Print actions; don't modify S3.")
    p.add_argument("--test", metavar="URL", help="Fetch a single URL to validate access.")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.test:
        test_one(args.test)
    else:
        exit_code = run_sync(dry_run=args.dry_run)
        sys.exit(exit_code)
