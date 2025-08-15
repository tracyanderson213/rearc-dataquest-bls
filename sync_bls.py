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
  BLS_RATE_LIMIT_RPS default: 3
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

# polite crawler defaults
USER_AGENT = os.environ.get(
    "BLS_USER_AGENT",
    "Mozilla/5.0 (compatible; bls-sync/1.0; +mailto:tracy.anderson@outlook.com; "
    "+https://github.com/tracyanderson213/rearc-dataquest-bls)"
)
CONTACT = os.environ.get("BLS_CONTACT", "tracy.anderson@outlook.com")
RPS = float(os.environ.get("BLS_RATE_LIMIT_RPS", "3"))
SLEEP = 1.0 / max(RPS, 0.1)
TIMEOUT = int(os.environ.get("BLS_HTTP_TIMEOUT", "60"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bls-sync")

# -----------------------------
# HTTP setup
# -----------------------------
session = requests.Session()

COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "From": CONTACT,
    "Accept": "*/*",
    "Connection": "keep-alive",
    # Optional hint; harmless if ignored:
    "Referer": "https://www.bls.gov/developers/"
}
session.headers.update(COMMON_HEADERS)
log.info("HTTP default headers: %s", session.headers)

# -----------------------------
# AWS S3 client
# -----------------------------
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 10, "mode": "standard"}))

# -----------------------------
# Helpers
# -----------------------------
def backoff(retries: int) -> float:
    """Exponential backoff with small jitter (1,2,4,8,16,30,60s...)."""
    base = min(60.0, 2 ** retries)
    return base + random.uniform(0.0, 0.5)

def save_error_response(url: str, resp: requests.Response) -> None:
    """Dump response to a timestamped file for debugging."""
    try:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"bls_error_{resp.status_code}_{ts}.txt"
        with open(fname, "w", encoding="utf-8", errors="replace") as f:
            f.write(f"URL: {url}\n")
            f.write(f"Final URL: {getattr(resp, 'url', url)}\n")
            f.write(f"Status: {resp.status_code}\n\n")
            f.write("=== Response Headers ===\n")
            for k, v in resp.headers.items():
                f.write(f"{k}: {v}\n")
            f.write("\n=== Body (first 10k chars) ===\n")
            f.write(resp.text[:10_000])
        log.warning("Saved error response to %s", fname)
    except Exception as e:
        log.warning("Could not write error dump: %s", e)

def get_url(url: str) -> requests.Response:
    retries = 0
    last_resp = None
    while True:
        try:
            time.sleep(SLEEP)
            resp = session.get(url, headers=COMMON_HEADERS, timeout=TIMEOUT, allow_redirects=True)
            last_resp = resp
            log.debug("GET %s -> %s", getattr(resp, "url", url), resp.status_code)
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            if retries >= 6:
                if last_resp is not None and getattr(last_resp, "ok", False) is False:
                    save_error_response(url, last_resp)
                raise
            wait = backoff(retries)
            log.warning("GET %s failed (%s). retrying in %.1fs", url, e, wait)
            time.sleep(wait)
            retries += 1

def head_url(url: str) -> requests.Response:
    retries = 0
    last_resp = None
    while True:
        try:
            time.sleep(SLEEP)
            resp = session.head(url, headers=COMMON_HEADERS, timeout=TIMEOUT, allow_redirects=True)
            last_resp = resp
            if resp.status_code in (405,):
                return get_url(url)  # head not allowed -> fall back
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                save_error_response(url, resp)
                raise requests.HTTPError(f"retryable {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            if retries >= 6:
                if last_resp is not None and getattr(last_resp, "ok", False) is False:
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
    resp = get_url(base_url)
    hrefs = HREF_RE.findall(resp.text)
    subs = []
    for h in hrefs:
        if h in ("../", "./"):
            continue
        if h.endswith("/"):
            subs.append(urljoin(base_url, h))
    return subs

def list_files(dir_url: str) -> List[Tuple[str, str]]:
    """Return (filename, file_url) tuples for a directory URL."""
    resp = get_url(dir_url)
    hrefs = HREF_RE.findall(resp.text)
    files = []
    for h in hrefs:
        if not h or h.endswith("/"):
            continue
        files.append((h, urljoin(dir_url, h)))
    return files

# -----------------------------
# S3 helpers
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
# Main sync
# -----------------------------
def run_sync(dry_run: bool=False) -> None:
    log.info("Starting BLS sync: %s -> s3://%s/%s/", BASE_URL, BUCKET, PREFIX)
    log.info("AWS region: %s  Bucket: %s  Prefix: %s  RPS: %s", REGION, BUCKET, PREFIX, RPS)

    time.sleep(2.0)  # gentle warm-up

    # 1) Discover
    subdirs = list_subdirs(BASE_URL)
    if not subdirs:
        log.error("No subdirectories discovered under %s", BASE_URL)
        sys.exit(2)

    source_map: Dict[str, Tuple[str, int, str]] = {}  # s3_key -> (url, len, lastmod)

    for d in sorted(subdirs):
        files = list_files(d)
        log.info("Found %d files in %s", len(files), d)
        for name, url in files:
            h = head_url(url)
            src_mtime_http = h.headers.get("Last-Modified", "")
            src_len = int(h.headers.get("Content-Length", "-1"))
            key = s3_key_for(url, BASE_URL, PREFIX)
            source_map[key] = (url, src_len, src_mtime_http)

    # 2) Upload new/changed
    uploaded = skipped = 0
    for key, (url, src_len, src_mtime_http) in sorted(source_map.items()):
        try:
            changed_now = upload_if_changed(BUCKET, key, url, src_len, src_mtime_http, dry_run=dry_run)
            if changed_now:
                uploaded += 1
            else:
                skipped += 1
        except Exception as e:
            log.error("Failed to sync %s -> s3://%s/%s : %s", url, BUCKET, key, e)
            continue

    # 3) Delete orphans
    s3_keys_now = set(list_s3_keys(BUCKET, PREFIX))
    source_keys = set(source_map.keys())
    orphans = sorted(s3_keys_now - source_keys)
    delete_keys(BUCKET, orphans, dry_run=dry_run)

    log.info("Sync complete. uploaded=%d skipped=%d deleted=%d total=%d dry_run=%s",
             uploaded, skipped, len(orphans), len(source_map), dry_run)

# -----------------------------
# Single URL test mode
# -----------------------------
def test_one(url: str) -> None:
    log.info("Testing single URL: %s", url)
    r = get_url(url)
    log.info("OK %s -> %s bytes", url, len(r.content))

# -----------------------------
# Entrypoint
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Mirror BLS time series to S3.")
    p.add_argument("--dry-run", action="store_true", help="Print actions; don't modify S3.")
    p.add_argument("--test", metavar="URL", help="Fetch a single URL to validate access.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.test:
        test_one(args.test)
    else:
        run_sync(dry_run=args.dry_run)
