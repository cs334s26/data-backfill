#!/usr/bin/env python3
"""
ingest_regulations.py

Usage (bash):
  chmod +x ingest_regulations.py
  ./ingest_regulations.py <agency>

Arguments:
  $1  agency   Required. The agency prefix to process (e.g. CMS, EPA, FDA).
               This maps to s3://mirrulations/raw-data/<agency>/

The script walks every docket under the agency, reads the docket JSON from
  s3://mirrulations/raw-data/<agency>/<docket-id>/text-<docket-id>/docket/<docket-id>.json
and for each HTML/HTM URL found:
  1. Checks if the document is already in OpenSearch (skips if so)
  2. Downloads the HTML from regulations.gov
  3. Ingests the parsed text into OpenSearch index: documents_text

Records within each docket are processed in REVERSE order (newest first).

Required environment variables:
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_REGION             (default: us-east-1)
  OPENSEARCH_HOST        e.g. https://<collection-id>.us-east-1.aoss.amazonaws.com

  Note: OpenSearch Serverless uses IAM auth (SigV4) — no username/password needed.
"""

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
from bs4 import BeautifulSoup
from opensearchpy import OpenSearch, RequestsHttpConnection, NotFoundError, AWSV4SignerAuth
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
S3_BUCKET = "mirrulations"
S3_PREFIX = "raw-data"

CONFIG = {
    "opensearch_host":  os.getenv("OPENSEARCH_HOST", ""),
    "opensearch_index": "documents_text",   # hardcoded — do not change
    "aws_region":       os.getenv("AWS_REGION", "us-east-1"),
}

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
    """Raised when regulations.gov blocks a download request."""
    pass

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client("s3", region_name=CONFIG["aws_region"])

# ---------------------------------------------------------------------------
# OpenSearch client
# ---------------------------------------------------------------------------

def get_opensearch_client():
    parsed  = urlparse(CONFIG["opensearch_host"])
    host    = parsed.hostname
    port    = parsed.port or 443

    # OpenSearch Serverless uses IAM SigV4 signing — no username/password
    credentials = boto3.Session().get_credentials()
    auth        = AWSV4SignerAuth(credentials, CONFIG["aws_region"], "aoss")

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
    )

# ---------------------------------------------------------------------------
# OpenSearch helpers
# ---------------------------------------------------------------------------

def ensure_index(os_client):
    """
    OpenSearch Serverless auto-creates indexes on first write.
    No need to explicitly create it — just log and move on.
    """
    log.info("Index '%s' will be created automatically on first write if needed", CONFIG["opensearch_index"])


def document_exists_in_opensearch(os_client, document_id: str) -> bool:
    """Check if a document is already ingested by its ID."""
    try:
        os_client.get(index=CONFIG["opensearch_index"], id=document_id)
        return True
    except NotFoundError:
        return False


def ingest_document(os_client, docket_id: str, document_id: str, text: str):
    doc = {
        "docketId":     docket_id,
        "documentId":   document_id,
        "documentText": text,
    }
    os_client.index(index=CONFIG["opensearch_index"], id=document_id, body=doc)
    log.info("Ingested '%s' → OpenSearch index '%s'", document_id, CONFIG["opensearch_index"])

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def list_dockets(s3, agency: str):
    """
    List all docket IDs under s3://mirrulations/raw-data/<agency>/
    Returns a list of docket ID strings e.g. ['CMS-2026-1420', ...]
    """
    prefix   = f"{S3_PREFIX}/{agency}/"
    paginator = s3.get_paginator("list_objects_v2")
    dockets  = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] looks like "raw-data/CMS/CMS-2026-1420/"
            docket_id = cp["Prefix"].rstrip("/").split("/")[-1]
            dockets.append(docket_id)

    return dockets


def list_document_jsons(s3, agency: str, docket_id: str):
    """
    List all document JSON files under:
      s3://mirrulations/raw-data/<agency>/<docket-id>/text-<docket-id>/documents/
    Returns a list of S3 keys for each document JSON.
    """
    prefix    = f"{S3_PREFIX}/{agency}/{docket_id}/text-{docket_id}/documents/"
    paginator = s3.get_paginator("list_objects_v2")
    keys      = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Only grab .json files, not the .htm/.html content files
            if key.endswith(".json"):
                keys.append(key)

    return keys


def read_document_json(s3, key: str):
    """
    Read and parse a single document JSON from S3.
    Returns parsed JSON or None on error.
    """
    try:
        obj  = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            log.warning("Document JSON not found: s3://%s/%s", S3_BUCKET, key)
            return None
        raise
    except json.JSONDecodeError as e:
        log.error("Failed to parse JSON at s3://%s/%s: %s", S3_BUCKET, key, e)
        return None

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def get_http_session():
    session = requests.Session()
    retry   = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

# ---------------------------------------------------------------------------
# Block detection
# ---------------------------------------------------------------------------

def check_if_blocked(resp: requests.Response, url: str) -> bool:
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
        log.warning("  Action: skipping this document — not ingested")
        log.warning("=" * 60)

    return blocked

# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_html(session, url: str) -> bytes:
    log.info("Downloading %s", url)
    resp = session.get(url, timeout=30)
    if check_if_blocked(resp, url):
        raise BlockedBySourceError(f"Blocked by regulations.gov: {url}")
    resp.raise_for_status()
    return resp.content


def extract_text(html_bytes: bytes) -> str:
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_html_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".html") or path.endswith(".htm")

# ---------------------------------------------------------------------------
# Process a single docket
# ---------------------------------------------------------------------------

def process_document(data: dict, docket_id: str, session, os_client):
    """
    Process a single document JSON. Structure:
    { "data": { "attributes": { "fileFormats": [...], ... }, "id": "DOC-ID" } }
    """
    attributes  = data.get("data", {}).get("attributes", {})
    document_id = data.get("data", {}).get("id", "unknown-document")
    rec_docket_id = attributes.get("docketId") or docket_id

    file_formats = attributes.get("fileFormats", [])
    if not isinstance(file_formats, list):
        file_formats = [file_formats]

    html_urls = [
        fmt.get("fileUrl", "")
        for fmt in file_formats
        if is_html_url(fmt.get("fileUrl", ""))
    ]

    if not html_urls:
        log.debug("No HTML/HTM URLs for document '%s' — skipping", document_id)
        return

    for url in html_urls:
        url_suffix = Path(urlparse(url).path).stem
        doc_id_key = f"{document_id}-{url_suffix}" if len(html_urls) > 1 else document_id

        # Check OpenSearch first — skip if already ingested
        if document_exists_in_opensearch(os_client, doc_id_key):
            log.info("Already in OpenSearch, skipping: %s", doc_id_key)
            continue

        try:
            html_bytes = download_html(session, url)
            text       = extract_text(html_bytes)
            ingest_document(os_client, rec_docket_id, doc_id_key, text)

        except BlockedBySourceError:
            pass  # already logged in detail
        except requests.HTTPError as e:
            log.error("HTTP error downloading %s: %s", url, e)
        except Exception as e:
            log.error("Unexpected error for document '%s': %s", doc_id_key, e)

# ---------------------------------------------------------------------------
# List all agencies in the bucket
# ---------------------------------------------------------------------------

def list_agencies(s3):
    """
    List all agency folders under s3://mirrulations/raw-data/
    Returns a sorted list of agency names e.g. ['CMS', 'EPA', 'FDA', ...]
    """
    prefix    = f"{S3_PREFIX}/"
    paginator = s3.get_paginator("list_objects_v2")
    agencies  = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            agency = cp["Prefix"].rstrip("/").split("/")[-1]
            agencies.append(agency)

    return sorted(agencies)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(start: int, end: int):
    s3        = get_s3_client()
    os_client = get_opensearch_client()
    session   = get_http_session()

    ensure_index(os_client)

    log.info("Listing all agencies in s3://%s/%s/...", S3_BUCKET, S3_PREFIX)
    agencies = list_agencies(s3)

    if not agencies:
        log.error("No agencies found in s3://%s/%s/ — check bucket and credentials", S3_BUCKET, S3_PREFIX)
        sys.exit(1)

    total_agencies = len(agencies)
    log.info("Found %d agencies total:", total_agencies)
    for i, name in enumerate(agencies, start=1):
        log.info("  [%d] %s", i, name)

    # Validate range (1-based, inclusive on both ends)
    if start < 1 or start > total_agencies:
        log.error("start index %d is out of range (1-%d)", start, total_agencies)
        sys.exit(1)
    if end > total_agencies:
        log.warning("end index %d exceeds total — clamping to %d", end, total_agencies)
        end = total_agencies

    selected = agencies[start - 1:end]  # convert 1-based to 0-based slice
    log.info("Processing agencies %d-%d: %s", start, end, ", ".join(selected))

    for agency_idx, agency in enumerate(selected, start=start):
        log.info("*** Agency %d / %d: %s ***", agency_idx, total_agencies, agency)

        dockets = list_dockets(s3, agency)
        if not dockets:
            log.warning("No dockets found for agency '%s' — skipping", agency)
            continue

        log.info("Found %d dockets for %s", len(dockets), agency)

        for d_idx, docket_id in enumerate(dockets, start=1):
            log.info("=== Docket %d / %d: %s ===", d_idx, len(dockets), docket_id)

            doc_keys = list_document_jsons(s3, agency, docket_id)
            if not doc_keys:
                log.warning("No document JSONs found for docket '%s' — skipping", docket_id)
                continue

            # Reverse: newest documents first
            doc_keys = list(reversed(doc_keys))
            log.info("Found %d documents in docket %s", len(doc_keys), docket_id)

            for doc_key in doc_keys:
                data = read_document_json(s3, doc_key)
                if data is None:
                    continue
                process_document(data, docket_id, session, os_client)

    log.info("Done. Processed agencies %d-%d.", start, end)

# ---------------------------------------------------------------------------
# Entry point  —  $1 = start index, $2 = end index (1-based, inclusive)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./ingest_regulations.py <start> [end]")
        print()
        print("  $1  start   Agency index to start from (1-based)")
        print("  $2  end     Agency index to stop at, inclusive (default: same as start)")
        print()
        print("  Agencies are the folders found in s3://mirrulations/raw-data/")
        print()
        print("  Examples:")
        print("    ./ingest_regulations.py 1        # process agency #1 only")
        print("    ./ingest_regulations.py 1 15     # process agencies 1 through 15")
        print("    ./ingest_regulations.py 16 30    # process agencies 16 through 30")
        print()
        print("  Tip: run with any number to see the full numbered agency list first.")
        sys.exit(1)

    try:
        start = int(sys.argv[1])
    except ValueError:
        print(f"Error: start must be an integer, got '{sys.argv[1]}'")
        sys.exit(1)

    try:
        end = int(sys.argv[2]) if len(sys.argv) > 2 else start
    except ValueError:
        print(f"Error: end must be an integer, got '{sys.argv[2]}'")
        sys.exit(1)

    run(start, end)