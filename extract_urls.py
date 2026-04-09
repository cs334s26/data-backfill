#!/usr/bin/env python3
"""
extract_urls.py

Walks s3://mirrulations/raw-data/ and extracts up to 500 HTM/HTML URLs
from document JSONs, saving them to a file called urls.txt.

Usage:
  chmod +x extract_urls.py
  ./extract_urls.py

Output:
  urls.txt — one URL per line, up to 500 URLs
"""

import json
import os
import sys
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

S3_BUCKET  = "mirrulations"
S3_PREFIX  = "raw-data"
TARGET     = 500
OUTPUT     = "urls.txt"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def is_html_url(url: str) -> bool:
    url_lower = url.lower()
    return url_lower.endswith(".html") or url_lower.endswith(".htm")


def list_agencies(s3) -> list:
    prefix    = f"{S3_PREFIX}/"
    paginator = s3.get_paginator("list_objects_v2")
    agencies  = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            agencies.append(cp["Prefix"].rstrip("/").split("/")[-1])
    return sorted(agencies)


def list_dockets(s3, agency: str) -> list:
    prefix    = f"{S3_PREFIX}/{agency}/"
    paginator = s3.get_paginator("list_objects_v2")
    dockets   = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dockets.append(cp["Prefix"].rstrip("/").split("/")[-1])
    return dockets


def list_document_jsons(s3, agency: str, docket_id: str) -> list:
    prefix    = f"{S3_PREFIX}/{agency}/{docket_id}/text-{docket_id}/documents/"
    paginator = s3.get_paginator("list_objects_v2")
    keys      = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def extract_urls_from_json(s3, key: str) -> list:
    try:
        obj        = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data       = json.loads(obj["Body"].read().decode("utf-8"))
        attributes = data.get("data", {}).get("attributes", {})
        formats    = attributes.get("fileFormats", [])
        if not isinstance(formats, list):
            formats = [formats]
        return [
            fmt.get("fileUrl", "")
            for fmt in formats
            if is_html_url(fmt.get("fileUrl", ""))
        ]
    except (ClientError, json.JSONDecodeError, KeyError):
        return []


def main():
    s3      = boto3.client("s3", region_name=AWS_REGION)
    urls    = []

    log.info("Extracting up to %d HTML/HTM URLs from s3://%s/%s/", TARGET, S3_BUCKET, S3_PREFIX)

    agencies = list_agencies(s3)
    log.info("Found %d agencies", len(agencies))

    for agency in agencies:
        if len(urls) >= TARGET:
            break

        dockets = list_dockets(s3, agency)
        for docket_id in dockets:
            if len(urls) >= TARGET:
                break

            doc_keys = list_document_jsons(s3, agency, docket_id)
            for key in doc_keys:
                if len(urls) >= TARGET:
                    break

                found = extract_urls_from_json(s3, key)
                urls.extend(found)

                if len(urls) % 50 == 0 and len(urls) > 0:
                    log.info("Collected %d URLs so far...", len(urls))

    urls = urls[:TARGET]
    log.info("Done. Writing %d URLs to %s", len(urls), OUTPUT)

    with open(OUTPUT, "w") as f:
        for url in urls:
            f.write(url + "\n")

    log.info("Saved to %s", OUTPUT)


if __name__ == "__main__":
    main()
