#!/usr/bin/env python3
"""
s3_backfill.py

Walks the local data folder, reads document JSONs, downloads HTML files
from regulations.gov, and uploads them to the mirrulations S3 bucket
next to their corresponding JSON files.

S3 path mirrors the local path:
  Local:  /mnt/data/data/<agency>/<docket-id>/text-<docket-id>/<doc-id>.json
  S3:     s3://mirrulations/raw-data/<agency>/<docket-id>/text-<docket-id>/<doc-id>_content.html

Usage:
  chmod +x s3_backfill.py
  ./s3_backfill.py <start> [end] [--since YYYY-MM-DD]

Arguments:
  $1  start           Agency index to start from (1-based)
  $2  end             Agency index to stop at, inclusive (default: same as start)
  --since YYYY-MM-DD  Only process documents posted on or after this date

Required environment variables:
  AWS_REGION          (default: us-east-1)
  LOCAL_DATA_PATH     Path to local data folder (default: /mnt/data/data)
"""

import argparse
import datetime
import json
import os
import sys
import logging
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
S3_BUCKET       = "mirrulations"
S3_PREFIX       = "raw-data"
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "/mnt/data/data")
AWS_REGION      = os.getenv("AWS_REGION", "us-east-1")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; regulations-ingester/1.0; "
        "+https://github.com/your-org/regulations-ingester)"
    )
}

BLOCK_INDICATORS = [
    "Access Denied",
    "403 Forbidden",
    "You have been blocked",
    "Request blocked",
    "unusual traffic",
    "captcha",
    "CAPTCHA",
    "blocked by",
    "security check",
    "Please verify you are a human",
]

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class BlockedBySourceError(Exception):
    pass

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def file_exists_in_s3(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_to_s3(s3, key: str, content: bytes):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=content, ContentType="text/html")
    log.info("Uploaded s3://%s/%s", S3_BUCKET, key)

# ---------------------------------------------------------------------------
# Local file helpers
# ---------------------------------------------------------------------------

def list_agencies_local():
    try:
        return sorted([
            name for name in os.listdir(LOCAL_DATA_PATH)
            if os.path.isdir(os.path.join(LOCAL_DATA_PATH, name))
        ])
    except OSError as e:
        log.error("Cannot read local data path %s: %s", LOCAL_DATA_PATH, e)
        return []


def list_dockets_local(agency: str):
    agency_path = os.path.join(LOCAL_DATA_PATH, agency)
    if not os.path.isdir(agency_path):
        return []
    return sorted([
        name for name in os.listdir(agency_path)
        if os.path.isdir(os.path.join(agency_path, name))
    ])


def list_document_jsons_local(agency: str, docket_id: str):
    # Check both with and without documents/ subfolder
    base_path = os.path.join(LOCAL_DATA_PATH, agency, docket_id, f"text-{docket_id}")
    docs_path = os.path.join(base_path, "documents")

    search_path = docs_path if os.path.isdir(docs_path) else base_path
    if not os.path.isdir(search_path):
        return []

    json_files = []
    for root, dirs, files in os.walk(search_path):
        for fname in files:
            if fname.endswith(".json"):
                json_files.append(os.path.join(root, fname))
    return sorted(json_files)


def read_document_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to parse JSON at %s: %s", path, e)
        return None


def s3_key_for_document(local_json_path: str, url: str) -> str:
    """
    Build the S3 key for an HTML file based on the local JSON path.
    Mirrors the local folder structure under raw-data/ with a documents/ subfolder.
    e.g. /mnt/data/data/FAA/FAA-2012-0495/text-FAA-2012-0495/FAA-2012-0495-0001.json
    → raw-data/FAA/FAA-2012-0495/text-FAA-2012-0495/documents/FAA-2012-0495-0001_content.html
    """
    # Get relative path from LOCAL_DATA_PATH
    rel_path  = os.path.relpath(local_json_path, LOCAL_DATA_PATH)
    # Get the directory and base filename
    dir_part  = os.path.dirname(rel_path)
    base_name = os.path.splitext(os.path.basename(rel_path))[0]
    # Get the file extension from the URL
    url_path  = urlparse(url).path
    ext       = os.path.splitext(url_path)[1]  # .html or .htm
    # Build the S3 key with documents/ subfolder
    s3_key    = f"{S3_PREFIX}/{dir_part}/documents/{base_name}_content{ext}"
    return s3_key

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def get_http_session():
    session = requests.Session()
    retry   = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

# ---------------------------------------------------------------------------
# Block detection
# ---------------------------------------------------------------------------

def check_if_blocked(resp, url: str) -> bool:
    blocked = False
    reason  = None

    if resp.status_code == 403:
        reason  = "HTTP 403 Forbidden"
        blocked = True
    elif resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After", "unknown")
        reason  = f"HTTP 429 Too Many Requests (Retry-After: {retry_after}s)"
        blocked = True
    elif resp.status_code == 503:
        reason  = "HTTP 503 Service Unavailable"
        blocked = True

    if not blocked:
        body_sample = resp.content[:4000].decode("utf-8", errors="ignore")
        for phrase in BLOCK_INDICATORS:
            if phrase.lower() in body_sample.lower():
                reason  = f'block page detected (matched: "{phrase}")'
                blocked = True
                break

    if blocked:
        log.warning("=" * 60)
        log.warning("BLOCKED BY REGULATIONS.GOV")
        log.warning("  URL:    %s", url)
        log.warning("  Reason: %s", reason)
        log.warning("  Time:   %s", datetime.datetime.now().isoformat())
        log.warning("  Action: skipping this document")
        log.warning("=" * 60)

    return blocked

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_html(session, url: str) -> bytes:
    log.info("Downloading %s", url)
    resp = session.get(url, timeout=30)
    if check_if_blocked(resp, url):
        raise BlockedBySourceError(f"Blocked: {url}")
    resp.raise_for_status()
    return resp.content

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_html_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".html")

# ---------------------------------------------------------------------------
# Process a single document
# ---------------------------------------------------------------------------

def process_document(data: dict, local_json_path: str, s3, session, since_date=None):
    attributes  = data.get("data", {}).get("attributes", {})
    document_id = data.get("data", {}).get("id", "unknown-document")

    # Filter by postedDate if --since was specified
    if since_date is not None:
        posted_raw = attributes.get("postedDate") or attributes.get("modifyDate")
        if posted_raw:
            try:
                posted = datetime.date.fromisoformat(posted_raw[:10])
                if posted < since_date:
                    log.debug("Skipping '%s' — posted %s before --since %s",
                              document_id, posted, since_date)
                    return
            except ValueError:
                pass

    file_formats = attributes.get("fileFormats", [])
    if not isinstance(file_formats, list):
        file_formats = [file_formats]

    html_urls = [
        fmt.get("fileUrl", "")
        for fmt in file_formats
        if fmt and is_html_url(fmt.get("fileUrl", ""))
    ]

    if not html_urls:
        log.debug("No HTML URLs for document '%s' — skipping", document_id)
        return

    for url in html_urls:
        s3_key = s3_key_for_document(local_json_path, url)

        # Skip if already in S3
        if file_exists_in_s3(s3, s3_key):
            log.info("Already in S3, skipping: %s", s3_key)
            continue

        try:
            html_bytes = download_html(session, url)
            upload_to_s3(s3, s3_key, html_bytes)

        except BlockedBySourceError:
            pass
        except requests.HTTPError as e:
            log.error("HTTP error downloading %s: %s", url, e)
        except Exception as e:
            log.error("Unexpected error for document '%s': %s", document_id, e)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(start: int, end: int, since_date=None):
    s3      = get_s3_client()
    session = get_http_session()

    log.info("Using local data from: %s", LOCAL_DATA_PATH)
    log.info("Uploading to: s3://%s/%s/", S3_BUCKET, S3_PREFIX)

    agencies = list_agencies_local()
    if not agencies:
        log.error("No agencies found in %s", LOCAL_DATA_PATH)
        sys.exit(1)

    total_agencies = len(agencies)
    log.info("Found %d agencies total", total_agencies)

    if start < 1 or start > total_agencies:
        log.error("start index %d is out of range (1-%d)", start, total_agencies)
        sys.exit(1)
    if end > total_agencies:
        log.warning("end index %d exceeds total — clamping to %d", end, total_agencies)
        end = total_agencies

    selected = agencies[start - 1:end]
    log.info("Processing agencies %d-%d: %s", start, end, ", ".join(selected))
    if since_date:
        log.info("Filtering to documents posted on or after: %s", since_date)

    for agency_idx, agency in enumerate(selected, start=start):
        log.info("*** Agency %d / %d: %s ***", agency_idx, total_agencies, agency)

        dockets = list_dockets_local(agency)
        if not dockets:
            log.warning("No dockets found for agency '%s' — skipping", agency)
            continue

        log.info("Found %d dockets for %s", len(dockets), agency)

        for d_idx, docket_id in enumerate(dockets, start=1):
            log.info("=== Docket %d / %d: %s ===", d_idx, len(dockets), docket_id)

            doc_keys = list_document_jsons_local(agency, docket_id)
            if not doc_keys:
                log.warning("No document JSONs found for docket '%s' — skipping", docket_id)
                continue

            doc_keys = list(reversed(doc_keys))
            log.info("Found %d documents in docket %s", len(doc_keys), docket_id)

            for doc_key in doc_keys:
                data = read_document_json(doc_key)
                if data is None:
                    continue
                process_document(data, doc_key, s3, session, since_date=since_date)

    log.info("Done. Processed agencies %d-%d.", start, end)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download HTML files from regulations.gov and upload to S3."
    )
    parser.add_argument("start", type=int,
                        help="Agency index to start from (1-based)")
    parser.add_argument("end", type=int, nargs="?", default=None,
                        help="Agency index to stop at, inclusive (default: same as start)")
    parser.add_argument("--since", type=str, default=None,
                        metavar="YYYY-MM-DD",
                        help="Only process documents posted on or after this date")

    args = parser.parse_args()

    start = args.start
    end   = args.end if args.end is not None else start

    since_date = None
    if args.since:
        try:
            since_date = datetime.date.fromisoformat(args.since)
        except ValueError:
            print(f"Error: --since date must be in YYYY-MM-DD format, got '{args.since}'")
            sys.exit(1)

    run(start, end, since_date=since_date)
