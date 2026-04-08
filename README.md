# Regulations Ingester

Downloads HTML documents from `regulations.gov`, optionally stores them in S3,
and ingests the parsed text into an AWS OpenSearch index called `documents_text`.

To backfill with multiple EC2 instances at once, each instance is given a
**slice** of the JSON file via `start` and `end` arguments. There is no
coordination service needed — OpenSearch upserts by document ID, so even if
two instances accidentally overlap on the same record, no duplicates or
corruption occur.

---

## Files

| File | Purpose |
|---|---|
| `ingest_regulations.py` | Main script — run on each EC2 instance |
| `requirements.txt` | Python dependencies |
| `README.md` | This document |

---

## AWS Permissions Setup (do this once)

All 20 users must have IAM accounts in the main AWS account. The steps below
set up a shared IAM group so permissions are managed in one place.

### 1. Create an IAM group

1. Go to **IAM** in the AWS console
2. Click **User groups** → **Create group**
3. Name it `regulations-ingesters`
4. Click **Create group**

### 2. Add all users to the group

1. Click into the `regulations-ingesters` group
2. Go to the **Users** tab → **Add users**
3. Select all 20 users and click **Add users**

### 3. Attach an OpenSearch permission policy to the group

1. Click into the group → **Permissions** tab
2. Click **Add permissions** → **Create inline policy**
3. Click the **JSON** tab, clear the editor, and paste this:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "es:*",
      "Resource": "arn:aws:es:us-east-1:MAIN_ACCOUNT_ID:domain/DOMAIN_NAME/*"
    }
  ]
}
```

4. Replace `MAIN_ACCOUNT_ID` and `DOMAIN_NAME` with the real values
5. Name the policy `opensearch-ingest-access` → **Create policy**

### 4. Add the group to the OpenSearch access policy

The domain owner needs to do this:

1. Go to **Amazon OpenSearch Service** → click the domain
2. **Security configuration** tab → **Access policy** → **Edit**
3. Add this statement (replacing the ARN with the real group ARN):

```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::MAIN_ACCOUNT_ID:group/regulations-ingesters"
  },
  "Action": "es:*",
  "Resource": "arn:aws:es:us-east-1:MAIN_ACCOUNT_ID:domain/DOMAIN_NAME/*"
}
```

4. Click **Save changes**

### 5. Map the group in OpenSearch Dashboards (if fine-grained access control is on)

1. Open **OpenSearch Dashboards** (URL is on the domain page)
2. Log in as the master user
3. Go to **Security** → **Roles** → click `all_access`
4. **Mapped users** tab → **Map users**
5. Under **Backend roles** paste the group ARN:
   `arn:aws:iam::MAIN_ACCOUNT_ID:group/regulations-ingesters`
6. Click **Map**

### 6. Each user generates their own access keys

Each person does this once in the main AWS account:

1. Go to **IAM** → **Users** → click their username
2. **Security credentials** tab → **Create access key**
3. Select **Application running outside AWS**
4. Download or copy the **Access key ID** and **Secret access key**

---

## EC2 Instance Setup (each person does this on their own instance)

### Step 1 — Launch the instance

1. Go to **EC2** in the AWS console → **Launch instance**
2. Name it `regulations-ingester`
3. Select **Amazon Linux 2** as the OS
4. Select **t3.medium** as the instance type
5. Create or select a **key pair** so you can SSH in
6. Under **Network settings** make sure **Allow SSH traffic** is checked
7. Click **Launch instance**

### Step 2 — SSH into the instance

Copy the **Public IPv4 address** from the instance page, then connect:

```bash
ssh -i your-key.pem ec2-user@YOUR_INSTANCE_IP
```

If you get a permissions error on the key file first run:
```bash
chmod 400 your-key.pem
```

### Step 3 — Install Python dependencies

```bash
sudo yum update -y
sudo yum install -y python3-pip git
pip3 install requests beautifulsoup4 opensearch-py urllib3
```

### Step 4 — Copy the script and JSON file onto the instance

Run this from your **local machine**:
```bash
scp -i your-key.pem ingest_regulations.py ec2-user@YOUR_INSTANCE_IP:~/
scp -i your-key.pem docs.json ec2-user@YOUR_INSTANCE_IP:~/
```

Or if the script is in a git repo:
```bash
git clone https://github.com/your-org/your-repo.git
```

### Step 5 — Set up the .env file

Create a `.env` file in your home directory to store your credentials:

```bash
nano ~/.env
```

Paste the following into the file, filling in your real values:

```bash
# Required
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
export OPENSEARCH_HOST=https://the-domain-endpoint

# Optional — only needed if USE_S3=true
# export USE_S3=true
# export S3_BUCKET_NAME=my-regulations-bucket
```

Save and close the file (`Ctrl+O`, `Enter`, `Ctrl+X` in nano).

Lock down the file so only you can read it:
```bash
chmod 600 ~/.env
```

Load the variables into your session:
```bash
source ~/.env
```

To make this automatic on every login so you never have to run `source` manually, add it to `~/.bashrc`:
```bash
echo 'source ~/.env' >> ~/.bashrc
```

Verify the variables loaded correctly:
```bash
echo $OPENSEARCH_HOST
echo $AWS_ACCESS_KEY_ID
```

### Step 6 — Make the script executable and verify the connection

```bash
chmod +x ingest_regulations.py

# Test OpenSearch is reachable
curl "$OPENSEARCH_HOST/_cat/indices?v"
```

A successful response shows a list of indices. A `403` means permissions
aren't applied yet. `connection refused` means the host URL is wrong.

---

## Usage

```
./ingest_regulations.py <file> [start] [end]
```

| Argument | Required | Description |
|---|---|---|
| `$1` file | Yes | Path to the input JSON file |
| `$2` start | No | First record index to process, 0-based (default: `0`) |
| `$3` end | No | Record index to stop at, **exclusive** (default: end of file) |

Within the selected range, records are always processed **newest first**
(reverse order), so the most recent data is ingested before older data.

### Examples

Process the entire file:
```bash
./ingest_regulations.py docs.json
```

Process records 0 through 499:
```bash
./ingest_regulations.py docs.json 0 500
```

Process records 500 through 999:
```bash
./ingest_regulations.py docs.json 500 1000
```

---

## Running on multiple EC2 instances

### Step 1 — Find the total record count

Run this once on any instance:
```bash
python3 -c "import json; f=open('docs.json'); d=json.load(f); print(len(d))"
```
Example output: `3000`

### Step 2 — Divide the range evenly

With 3000 records and 20 people, each person gets 150 records:

| Person | Command |
|---|---|
| Person 1 | `./ingest_regulations.py docs.json 0 150` |
| Person 2 | `./ingest_regulations.py docs.json 150 300` |
| Person 3 | `./ingest_regulations.py docs.json 300 450` |
| ... | ... |
| Person 20 | `./ingest_regulations.py docs.json 2850 3000` |

### Step 3 — Run with nohup so it keeps going if SSH drops

```bash
nohup ./ingest_regulations.py docs.json 0 150 > my_run.log 2>&1 &
```

Check progress:
```bash
tail -f my_run.log
```

Check if it's still running:
```bash
ps aux | grep ingest
```

---

## What happens if ranges overlap?

It is safe to overlap ranges. OpenSearch uses the `documentId` as the document
ID, which means writes are **upserts** — the last writer wins but the data is
identical, so there are no duplicates or corruption.

---

## What happens if the script gets blocked?

When `regulations.gov` blocks a request (HTTP 403, 429, 503, or a CAPTCHA
page), the script logs a clear warning and **skips that document**. It does
not crash. The log will look like:

```
============================================================
BLOCKED BY REGULATIONS.GOV
  URL:    https://downloads.regulations.gov/.../content.html
  Reason: HTTP 429 Too Many Requests (Retry-After: 60s)
  Time:   2026-04-08T14:32:01.123456
  Action: skipping this document — not uploaded or ingested
============================================================
```

If you are getting blocked frequently, run fewer instances at once.

---

## Input JSON format

```json
[
  {
    "docketId":   "EPA-HQ-OAR-2021-0257",
    "documentId": "EPA-HQ-OAR-2021-0257-0001",
    "fileFormats": [
      {
        "fileUrl": "https://downloads.regulations.gov/EPA-HQ-OAR-2021-0257-0001/content.html"
      }
    ]
  }
]
```

Only `.html` and `.htm` URLs in `fileFormats` are processed. Other file
types (PDF, etc.) are ignored.

---

## OpenSearch index

Documents are always written to the `documents_text` index with this shape:

```json
{
  "docketId":     "EPA-HQ-OAR-2021-0257",
  "documentId":   "EPA-HQ-OAR-2021-0257-0001",
  "documentText": "Full parsed plain text of the HTML document..."
}
```

The index is created automatically if it does not exist.

---

## S3 storage (optional)

When `USE_S3=true`, raw HTML files are stored in S3 at:
```
s3://<S3_BUCKET_NAME>/html/<documentId>.html
```

On subsequent runs, if the file already exists in S3 it is loaded from there
instead of re-downloading from `regulations.gov`, saving bandwidth and reducing
the chance of being blocked.

S3 is **disabled by default**. To enable it:
```bash
export USE_S3=true
export S3_BUCKET_NAME=my-regulations-bucket
```

---

## Troubleshooting

**`connection refused` or can't reach OpenSearch**
Verify the `OPENSEARCH_HOST` URL and that your EC2 security group allows
outbound traffic on port 443.

**`403 Forbidden` from OpenSearch**
Your IAM user is not in the `regulations-ingesters` group, or the OpenSearch
access policy hasn't been updated yet. Check with whoever manages the domain.

**`No HTML/HTM URLs found for document`**
That record's `fileFormats` has no `.html` or `.htm` link. Expected for
PDF-only documents — the record is skipped automatically.

**Script exits immediately with a range error**
Your `start` index is >= the number of records in the file. Re-check the
record count and adjust your range.