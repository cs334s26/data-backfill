#!/usr/bin/env python3
"""
ingest_regulations.py

Usage (bash):
  chmod +x ingest_regulations.py
  ./ingest_regulations.py <path-to-json-file> [start] [end]

Arguments:
  $1  path-to-json-file   Required. Path to the input JSON file.
  $2  start               Optional. Index to start from (0-based, default: 0).
  $3  end                 Optional. Index to stop at, exclusive (default: end of file).

Examples:
  ./ingest_regulations.py docs.json            # process all records
  ./ingest_regulations.py docs.json 0 500      # process records 0-499
  ./ingest_regulations.py docs.json 500 1000   # process records 500-999

Records within the selected range are processed in REVERSE order (newest first).

Required environment variables:
  OPENSEARCH_HOST        e.g. https://search-my-domain.us-east-1.es.amazonaws.com
  OPENSEARCH_USER        basic auth username
  OPENSEARCH_PASSWORD    basic auth password

Optional S3 environment variables (only needed when USE_S3=true):
  USE_S3                 Set to "true" to enable S3 storage (default: false)
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_REGION             (default: us-east-1)
  S3_BUCKET_NAME
"""

import datetime
import json
import os
import sys
import logging
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from opensearchpy import OpenSearch, RequestsHttpConnection
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
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

CONFIG = {
    "opensearch_host":     os.getenv("OPENSEARCH_HOST", "https://localhost:9200"),
    "opensearch_index":    "documents_text",   # hardcoded — do not change
    "opensearch_user":     os.getenv("OPENSEARCH_USER", "admin"),
    "opensearch_password": os.getenv("OPENSEARCH_PASSWORD", "admin"),
    # S3 — only used when USE_S3=true
    "aws_region":          os.getenv("AWS_REGION", "us-east-1"),
    "s3_bucket":           os.getenv("S3_BUCKET_NAME", ""),
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
# OpenSearch client
# ---------------------------------------------------------------------------

def get_opensearch_client():
    parsed  = urlparse(CONFIG["opensearch_host"])
    use_ssl = parsed.scheme == "https"
    host    = parsed.hostname
    port    = parsed.port or (443 if use_ssl else 9200)

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=(CONFIG["opensearch_user"], CONFIG["opensearch_password"]),
        use_ssl=use_ssl,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

# ---------------------------------------------------------------------------
# S3 helpers  (only imported / used when USE_S3=true)
# ---------------------------------------------------------------------------

def get_s3_client():
    import boto3
    return boto3.client("s3", region_name=CONFIG["aws_region"])


def s3_key_for_document(document_id: str) -> str:
    return f"html/{document_id}.html"


def file_exists_in_s3(s3, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_to_s3(s3, bucket: str, key: str, content: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType="text/html")
    log.info("Uploaded s3://%s/%s", bucket, key)

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
        log.warning("  Action: skipping this document — not uploaded or ingested")
        log.warning("=" * 60)

    return blocked

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_html(session, url: str) -> bytes:
    log.info("Downloading %s", url)
    resp = session.get(url, timeout=30)
    if check_if_blocked(resp, url):
        raise BlockedBySourceError(f"Blocked by regulations.gov: {url}")
    resp.raise_for_status()
    return resp.content

# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def extract_text(html_bytes: bytes) -> str:
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())

# ---------------------------------------------------------------------------
# OpenSearch ingestion
# ---------------------------------------------------------------------------

def ensure_index(os_client, index: str):
    if os_client.indices.exists(index=index):
        return
    mapping = {
        "mappings": {
            "properties": {
                "docketId":     {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "documentId":   {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "documentText": {"type": "text"},
            }
        }
    }
    os_client.indices.create(index=index, body=mapping)
    log.info("Created OpenSearch index '%s'", index)


def ingest_document(os_client, index: str, docket_id: str, document_id: str, text: str):
    doc = {
        "docketId":     docket_id,
        "documentId":   document_id,
        "documentText": text,
    }
    os_client.index(index=index, id=document_id, body=doc)
    log.info("Ingested '%s' → OpenSearch index '%s'", document_id, index)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_html_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".html") or path.endswith(".htm")

# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_json_file(json_path: str, start: int, end: int | None):
    log.info("Reading input file: %s", json_path)
    with open(json_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    if isinstance(records, dict):
        records = [records]

    total_records = len(records)

    # Validate range
    if start < 0 or start >= total_records:
        log.error("start index %d is out of range (file has %d records)", start, total_records)
        sys.exit(1)

    if end is None:
        end = total_records
    elif end > total_records:
        log.warning("end index %d exceeds file length — clamping to %d", end, total_records)
        end = total_records

    # Slice then reverse: newest-first within the selected range
    slice_  = records[start:end]
    slice_  = list(reversed(slice_))
    count   = len(slice_)

    log.info(
        "Range: records %d–%d  (%d records) out of %d total — processing newest first",
        start, end - 1, count, total_records
    )
    log.info("S3 storage: %s", "ENABLED" if USE_S3 else "DISABLED (set USE_S3=true to enable)")

    os_client = get_opensearch_client()
    session   = get_http_session()
    s3        = get_s3_client() if USE_S3 else None

    ensure_index(os_client, CONFIG["opensearch_index"])

    for idx, record in enumerate(slice_, start=1):
        log.info("--- Record %d / %d  (file index %d) ---", idx, count, end - idx)

        docket_id   = record.get("docketId")   or record.get("docket_id",   "unknown-docket")
        document_id = record.get("documentId") or record.get("document_id", "unknown-document")

        file_formats = record.get("fileFormats", [])
        if not isinstance(file_formats, list):
            file_formats = [file_formats]

        html_urls = [
            fmt.get("fileUrl") or fmt.get("file_url", "")
            for fmt in file_formats
            if is_html_url(fmt.get("fileUrl") or fmt.get("file_url", ""))
        ]

        if not html_urls:
            log.warning("No HTML/HTM URLs found for document '%s' — skipping", document_id)
            continue

        for url in html_urls:
            url_suffix = Path(urlparse(url).path).stem
            doc_id_key = f"{document_id}-{url_suffix}" if len(html_urls) > 1 else document_id

            try:
                html_bytes = None

                if USE_S3:
                    s3_key = s3_key_for_document(doc_id_key)
                    if file_exists_in_s3(s3, CONFIG["s3_bucket"], s3_key):
                        log.info("Already in S3, loading from cache: %s", s3_key)
                        obj        = s3.get_object(Bucket=CONFIG["s3_bucket"], Key=s3_key)
                        html_bytes = obj["Body"].read()
                    else:
                        html_bytes = download_html(session, url)
                        upload_to_s3(s3, CONFIG["s3_bucket"], s3_key, html_bytes)
                else:
                    html_bytes = download_html(session, url)

                text = extract_text(html_bytes)
                ingest_document(os_client, CONFIG["opensearch_index"], docket_id, doc_id_key, text)

            except BlockedBySourceError:
                pass  # already logged in detail by check_if_blocked()
            except requests.HTTPError as e:
                log.error("HTTP error downloading %s: %s", url, e)
            except Exception as e:
                log.error("Unexpected error for document '%s': %s", doc_id_key, e)

    log.info("Done. Processed %d records (file range %d–%d).", count, start, end - 1)

# ---------------------------------------------------------------------------
# Entry point  —  filename passed as $1 (bash convention)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./ingest_regulations.py <path-to-json-file> [start] [end]")
        print("  $1  path-to-json-file")
        print("  $2  start index, 0-based (default: 0)")
        print("  $3  end index, exclusive  (default: end of file)")
        sys.exit(1)

    filename = sys.argv[1]  # $1

    if not os.path.isfile(filename):
        print(f"Error: file not found: {filename}")
        sys.exit(1)

    try:
        start = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    except ValueError:
        print(f"Error: start index must be an integer, got '{sys.argv[2]}'")
        sys.exit(1)

    try:
        end = int(sys.argv[3]) if len(sys.argv) > 3 else None
    except ValueError:
        print(f"Error: end index must be an integer, got '{sys.argv[3]}'")
        sys.exit(1)

    process_json_file(filename, start, end)
