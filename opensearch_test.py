#!/usr/bin/env python3
"""
opensearch_test.py

Downloads HTML files from urls.txt and attempts to ingest each one
into OpenSearch documents_text index one by one.
Halts immediately when a request fails and reports how many succeeded.

Usage:
  chmod +x opensearch_test.py
  ./opensearch_test.py [delay_seconds]

Arguments:
  $1  delay_seconds   Optional. Seconds to wait between requests (default: 0).

Required environment variables:
  OPENSEARCH_HOST   e.g. https://fyddewi9gmbuvxcee5nh.us-east-1.aoss.amazonaws.com
  AWS_REGION        (default: us-east-1)
"""

import sys
import os
import time
import logging
import datetime
import urllib.request
from urllib.parse import urlparse
from pathlib import Path

import boto3
import requests
from bs4 import BeautifulSoup
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

URL_FILE      = "urls.txt"
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
INDEX         = "documents_text"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; regulations-ingester/1.0; "
        "+https://github.com/your-org/regulations-ingester)"
    )
}


def get_opensearch_client():
    parsed      = urlparse(OPENSEARCH_HOST)
    host        = parsed.hostname
    port        = parsed.port or 443
    credentials = boto3.Session().get_credentials()
    auth        = AWSV4SignerAuth(credentials, AWS_REGION, "aoss")

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )


def extract_text(html_bytes: bytes) -> str:
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


def doc_id_from_url(url: str) -> str:
    """Extract document ID from URL e.g. ABMC-2005-0001-0001 from .../ABMC-2005-0001-0001/content.html"""
    parts = urlparse(url).path.strip("/").split("/")
    # URL format: /DOCKET-ID/content.html — doc ID is the second to last part
    return parts[-2] if len(parts) >= 2 else Path(urlparse(url).path).stem


def auto_terminate():
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            method="PUT"
        )
        token = urllib.request.urlopen(token_req, timeout=2).read().decode()
        instance_id_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token}
        )
        instance_id = urllib.request.urlopen(instance_id_req, timeout=2).read().decode()
        log.info("Terminating EC2 instance %s...", instance_id)
        ec2 = boto3.client("ec2", region_name=AWS_REGION)
        ec2.terminate_instances(InstanceIds=[instance_id])
        log.info("Termination request sent. Goodbye!")
    except Exception as e:
        log.warning("Could not auto-terminate instance: %s", e)
        log.warning("Please terminate the instance manually in the EC2 console.")


def main():
    if not OPENSEARCH_HOST:
        print("Error: OPENSEARCH_HOST environment variable is not set.")
        sys.exit(1)

    delay = 0.0
    if len(sys.argv) > 1:
        try:
            delay = float(sys.argv[1])
        except ValueError:
            print(f"Error: delay must be a number, got '{sys.argv[1]}'")
            sys.exit(1)

    try:
        with open(URL_FILE) as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {URL_FILE} not found. Run extract_urls.py first.")
        sys.exit(1)

    log.info("Loaded %d URLs from %s", len(urls), URL_FILE)
    log.info("Delay between requests: %ss", delay)
    log.info("OpenSearch host: %s", OPENSEARCH_HOST)
    log.info("Starting download + ingest test — will halt on first failure...")
    log.info("-" * 60)

    os_client = get_opensearch_client()
    session   = requests.Session()
    session.headers.update(HEADERS)

    success = 0
    start   = datetime.datetime.now()

    for i, url in enumerate(urls, start=1):
        document_id = doc_id_from_url(url)

        # Step 1 — Download from regulations.gov
        try:
            resp = session.get(url, timeout=30)
            if not resp.ok:
                elapsed = (datetime.datetime.now() - start).total_seconds()
                log.warning("=" * 60)
                log.warning("DOWNLOAD FAILED after %d successful ingests", success)
                log.warning("  URL:     %s", url)
                log.warning("  Status:  %d", resp.status_code)
                log.warning("  Elapsed: %.1fs", elapsed)
                log.warning("=" * 60)
                sys.exit(0)

            html_bytes = resp.content

        except requests.exceptions.RequestException as e:
            elapsed = (datetime.datetime.now() - start).total_seconds()
            log.warning("=" * 60)
            log.warning("DOWNLOAD ERROR after %d successful ingests", success)
            log.warning("  URL:     %s", url)
            log.warning("  Error:   %s", e)
            log.warning("  Elapsed: %.1fs", elapsed)
            log.warning("=" * 60)
            sys.exit(0)

        # Step 2 — Ingest into OpenSearch
        try:
            text = extract_text(html_bytes)
            doc  = {
                "docketId":     document_id.rsplit("-", 1)[0],
                "documentId":   document_id,
                "documentText": text,
            }
            os_client.index(index=INDEX, id=document_id, body=doc)

            success += 1
            elapsed = (datetime.datetime.now() - start).total_seconds()
            log.info(
                "[%d / %d] OK  %s  (%.1f req/min)",
                success, len(urls), document_id,
                (success / elapsed * 60) if elapsed > 0 else 0
            )

        except Exception as e:
            elapsed = (datetime.datetime.now() - start).total_seconds()
            log.warning("=" * 60)
            log.warning("OPENSEARCH INGEST FAILED after %d successful ingests", success)
            log.warning("  Document: %s", document_id)
            log.warning("  Error:    %s", e)
            log.warning("  Elapsed:  %.1fs", elapsed)
            log.warning("  Rate:     ~%.1f req/min", (success / elapsed * 60) if elapsed > 0 else 0)
            log.warning("=" * 60)
            sys.exit(0)

        if delay > 0:
            time.sleep(delay)

    # All done
    elapsed = (datetime.datetime.now() - start).total_seconds()
    log.info("=" * 60)
    log.info("ALL %d DOCUMENTS INGESTED SUCCESSFULLY!", success)
    log.info("  Total time: %.1fs", elapsed)
    log.info("  Rate: ~%.1f req/min", (success / elapsed * 60) if elapsed > 0 else 0)
    log.info("=" * 60)

    auto_terminate()


if __name__ == "__main__":
    main()
