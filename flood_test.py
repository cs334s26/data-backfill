#!/usr/bin/env python3
"""
flood_test.py

Reads urls.txt and downloads each URL one after the other.
Halts immediately when a request fails and reports how many
succeeded before being blocked.

Run this twice:
  Pass 1 — no delay:        ./flood_test.py
  Pass 2 — with 0.1s delay: ./flood_test.py 0.1

Usage:
  chmod +x flood_test.py
  ./flood_test.py [delay_seconds]

Arguments:
  $1  delay_seconds   Optional. Seconds to wait between requests (default: 0).
                      e.g. 0.1 for 100ms delay, 0.5 for 500ms delay.

Output:
  Prints a counter for each successful download.
  Halts and reports the count as soon as any request fails.
"""

import sys
import time
import logging
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

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

URL_FILE = "urls.txt"


def get_session():
    session = requests.Session()
    # No automatic retries — we want to detect failures immediately
    adapter = HTTPAdapter(max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def check_if_blocked(resp) -> tuple:
    """Returns (blocked, reason)"""
    if resp.status_code == 403:
        return True, "HTTP 403 Forbidden"
    elif resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After", "unknown")
        return True, f"HTTP 429 Too Many Requests (Retry-After: {retry_after}s)"
    elif resp.status_code == 503:
        return True, "HTTP 503 Service Unavailable"
    elif not resp.ok:
        return True, f"HTTP {resp.status_code}"

    body = resp.content[:4000].decode("utf-8", errors="ignore")
    for phrase in BLOCK_INDICATORS:
        if phrase.lower() in body.lower():
            return True, f'Block page detected (matched: "{phrase}")'

    return False, None


def main():
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
    log.info("Starting downloads — will halt on first failure...")
    log.info("-" * 60)

    session   = get_session()
    success   = 0
    start     = datetime.datetime.now()

    for i, url in enumerate(urls, start=1):
        try:
            resp    = session.get(url, timeout=30)
            blocked, reason = check_if_blocked(resp)

            if blocked:
                elapsed = (datetime.datetime.now() - start).total_seconds()
                log.warning("=" * 60)
                log.warning("BLOCKED after %d successful downloads", success)
                log.warning("  Failed URL:  %s", url)
                log.warning("  Reason:      %s", reason)
                log.warning("  Time:        %s", datetime.datetime.now().isoformat())
                log.warning("  Elapsed:     %.1fs", elapsed)
                log.warning("  Rate:        ~%.1f requests/min", (success / elapsed * 60) if elapsed > 0 else 0)
                log.warning("=" * 60)
                sys.exit(0)

            success += 1
            bytes_downloaded = len(resp.content)
            log.info("[%d / %d] OK  %d bytes  %s", success, len(urls), bytes_downloaded, url)

        except requests.exceptions.RequestException as e:
            elapsed = (datetime.datetime.now() - start).total_seconds()
            log.warning("=" * 60)
            log.warning("REQUEST FAILED after %d successful downloads", success)
            log.warning("  Failed URL: %s", url)
            log.warning("  Error:      %s", e)
            log.warning("  Elapsed:    %.1fs", elapsed)
            log.warning("=" * 60)
            sys.exit(0)

        if delay > 0:
            time.sleep(delay)

    # Made it through all URLs without being blocked
    elapsed = (datetime.datetime.now() - start).total_seconds()
    log.info("=" * 60)
    log.info("ALL %d DOWNLOADS SUCCEEDED — never blocked!", success)
    log.info("  Total time: %.1fs", elapsed)
    log.info("  Rate: ~%.1f requests/min", (success / elapsed * 60) if elapsed > 0 else 0)
    log.info("=" * 60)

    # Auto-terminate the EC2 instance when the test is done
    try:
        import urllib.request
        import boto3
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
        ec2 = boto3.client("ec2", region_name="us-east-1")
        ec2.terminate_instances(InstanceIds=[instance_id])
        log.info("Termination request sent. Goodbye!")
    except Exception as e:
        log.warning("Could not auto-terminate instance: %s", e)
        log.warning("Please terminate the instance manually in the EC2 console.")


if __name__ == "__main__":
    main()