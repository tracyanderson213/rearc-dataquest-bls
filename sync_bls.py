#!/usr/bin/env python3
import os
import re
import sys
import time
import hashlib
import logging
import itertools
from io import BytesIO
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from email.utils import parsedate_to_datetime

# -----------------------------
# Config (env overrides allowed)
# -----------------------------
BASE_URL = os.environ.get("BLS_BASE_URL", "https://download.bls.gov/pub/time.series/")
BUCKET   = os.environ.get("BLS_S3_BUCKET", "dataquest-gov-bls-timeseries")
PREFIX   = os.environ.get("BLS_S3_PREFIX", "bls")  # all objects live under this prefix
REGION   = os.environ.get("AWS_REGION", "us-east-2")

# polite crawler defaults
USER_AGENT = os.environ.get(
    "BLS_USER_AGENT",
    "dataquest-bls-sync/1.0 (+https://github.com/yourrepo; contact: you@example.com)"
)
RPS = float(os.environ.get("BLS_RATE_LIMIT_RPS", "3"))   # requests per second
SLEEP = 1.0 / max(RPS, 0.1)
TIMEOUT = int(os.environ.get("BLS_HTTP_TIMEOUT", "60"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bls-sync")

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "From": os.environ.get("BLS_CONTACT", "tracy.anderson@outlook.com"),
    "Accept": "*/*",
    "Connection": "keep-alive",
})


# -----------------------------
# HTTP helpers with backoff
# -----------------------------
def backoff(retries: int) -> float:
    return min(30.0, 2 ** retries)

def get_url(url: str) -> requests.Response:
    retries = 0
    while True:
        try:
            time.sleep(SLEEP)
            resp = session.get(url, timeout=TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            if retries >= 6:
                raise
            wait = backoff(retries)
            log.warning("GET %s failed (%s). retrying in %.1fs", url, e, wait)
            time.sleep(wait)
            retries += 1

def head_url(url: str) -> requests.Response:
    retries = 0
    while True:
        try:
            time.sleep(SLEEP)
            resp = session.head(url, timeout=TIMEOUT, allow_redirects=True)
            if resp.status_code in (405,):   # some dirs may not support HEAD -> fallback to GET
                return get_url(url)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            if retries >= 6:
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
        if h in ("../", "./"):  # parent/self
            continue
        if h.endswith('/'):     # directory
            subs.append(urljoin(base_url, h))
    return subs

def list_files(dir_url: str) -> List[Tuple[str, str]]:
    """Return (filename, file_url) tuples for a directory URL."""
    resp = get_url(dir_url)
    hrefs = HREF_RE.findall(resp.text)
    files = []
    for h in hrefs:
        if not h or h.endswith('/'):
            continue
        files.append((h, urljoin(dir_url, h)))
    return files


# -----------------------------
# S3 helpers
# -----------------------------
s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 10, "mode": "standard"}))

def s3_key_for(url: str, base_url: str, prefix: str) -> str:
    """
    Map a source URL to an S3 key under the given prefix.
    Example:
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
        if e.response["Error"]["Code"] in ("404", "NotFound"):
            return {}
        raise

def upload_if_changed(bucket: str, key: str, src_url: str, src_len: int, src_mtime_http: str) -> bool:
    """
    Upload only if changed. We compare the source Last-Modified + Content-Length
    to S3 object metadata. If identical, skip.
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
    # Safety check: sizes should match unless chunked
    if src_len >= 0 and len(data) != src_len:
        log.warning("Length mismatch for %s (HEAD=%s, body=%s). Proceeding.", src_url, src_len, len(data))

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

def delete_keys(bucket: str, keys: List[str]) -> None:
    if not keys:
        return
    # S3 delete in batches of 1000
    for chunk in (list(g) for k, g in itertools.groupby(enumerate(keys), key=lambda x: x[0] // 1000)):
        to_delete = [{"Key": v[1]} for v in chunk]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        log.info("DELETE %d objects", len(to_delete))


# -----------------------------
# Main sync
# -----------------------------
def main():
    log.info("Starting BLS sync: %s -> s3://%s/%s/", BASE_URL, BUCKET, PREFIX)
    log.info("AWS region: %s  Bucket: %s  Prefix: %s", REGION, BUCKET, PREFIX)

    # 1) Discover all files under BASE_URL
    subdirs = list_subdirs(BASE_URL)
    if not subdirs:
        log.error("No subdirectories discovered under %s", BASE_URL)
        sys.exit(2)

    source_map = {}  # s3_key -> (url, len, lastmod_str)

    for d in sorted(subdirs):
        files = list_files(d)
        log.info("Found %d files in %s", len(files), d)
        for name, url in files:
            # Use HEAD to obtain last-modified & content-length cheaply
            h = head_url(url)
            src_mtime_http = h.headers.get("Last-Modified", "")
            # Convert to epoch for potential future sorting (not required for compare)
            # epoch = parsedate_to_datetime(src_mtime_http).timestamp() if src_mtime_http else 0
            src_len = int(h.headers.get("Content-Length", "-1"))
            key = s3_key_for(url, BASE_URL, PREFIX)
            source_map[key] = (url, src_len, src_mtime_http)

    # 2) Upload new/changed
    uploaded = changed = skipped = 0
    for key, (url, src_len, src_mtime_http) in sorted(source_map.items()):
        try:
            changed_now = upload_if_changed(BUCKET, key, url, src_len, src_mtime_http)
            if changed_now:
                uploaded += 1
            else:
                skipped += 1
        except Exception as e:
            log.error("Failed to sync %s -> s3://%s/%s : %s", url, BUCKET, key, e)
            continue

    # 3) Delete orphans (present in S3 under prefix but not at source)
    s3_keys_now = set(list_s3_keys(BUCKET, PREFIX))
    source_keys = set(source_map.keys())
    orphans = sorted(s3_keys_now - source_keys)
    delete_keys(BUCKET, orphans)

    log.info("Sync complete. uploaded=%d skipped=%d deleted=%d total=%d",
             uploaded, skipped, len(orphans), len(source_map))

if __name__ == "__main__":
    main()
