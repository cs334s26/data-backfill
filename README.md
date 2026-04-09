# Regulations Ingester

Downloads HTML documents from `regulations.gov` and ingests the parsed text
into an AWS OpenSearch index called `documents_text`.

The script reads docket JSON files directly from the S3 bucket
`s3://mirrulations/raw-data/` and checks OpenSearch before downloading
anything — so documents that are already ingested are automatically skipped.

Each person runs the script with one or more **agency codes** (e.g. `CMS`,
`EPA`, `FDA`). The script walks every docket under that agency automatically.

---

## How it works

```
S3: s3://mirrulations/raw-data/<agency>/<docket-id>/text-<docket-id>/docket/<docket-id>.json
        ↓ read JSON
        ↓ find HTML URLs in fileFormats
        ↓ check OpenSearch — skip if already ingested
        ↓ download HTML from regulations.gov
        ↓ parse text
        ↓ ingest into OpenSearch (documents_text)
```

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

### 3. Attach permissions to the group

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
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::mirrulations",
        "arn:aws:s3:::mirrulations/*"
      ]
    }
  ]
}
```

4. Replace `MAIN_ACCOUNT_ID` and `DOMAIN_NAME` with the real values
5. Name the policy `regulations-ingester-access` → **Create policy**

### 4. Add the group to the OpenSearch access policy

The domain owner needs to do this:

1. Go to **Amazon OpenSearch Service** → click the domain
2. **Security configuration** tab → **Access policy** → **Edit**
3. Add this statement:

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

> **Note:** AWS only shows the secret access key once at creation time.
> Save it immediately — if you lose it you will need to create a new key.

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
pip3 install requests beautifulsoup4 opensearch-py urllib3 boto3
```

### Step 4 — Copy the script onto the instance

Run this from your **local machine**:
```bash
scp -i your-key.pem ingest_regulations.py ec2-user@YOUR_INSTANCE_IP:~/
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

Paste the following, filling in your real values:

```bash
# AWS credentials
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1

# OpenSearch — use the standard endpoint (not the FIPS one)
export OPENSEARCH_HOST=https://your-domain.us-east-1.es.amazonaws.com
export OPENSEARCH_USER=your-opensearch-username
export OPENSEARCH_PASSWORD=your-opensearch-password
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

To load automatically on every login:
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
curl -u "$OPENSEARCH_USER:$OPENSEARCH_PASSWORD" "$OPENSEARCH_HOST/_cat/indices?v"
```

A successful response shows a list of indices. A `403` means permissions
aren't applied yet. `connection refused` means the host URL is wrong.

---

## Usage

```
./ingest_regulations.py <agency>
```

| Argument | Required | Description |
|---|---|---|
| `$1` agency | Yes | Agency code to process, e.g. `CMS`, `EPA`, `FDA` |

The script walks **all dockets** under that agency automatically:
```
s3://mirrulations/raw-data/<agency>/
```

Within each docket, records are processed **newest first**.

### Examples

Process all CMS dockets:
```bash
./ingest_regulations.py CMS
```

Process all EPA dockets:
```bash
./ingest_regulations.py EPA
```

---

## Running on multiple EC2 instances

With 300 agencies and 20 people, divide the agencies between instances.
Coordinate who runs which agency beforehand to avoid overlap.

Example split:

| Person | Agencies |
|---|---|
| Person 1 | `CMS` `EPA` ... |
| Person 2 | `FDA` `DOT` ... |
| ... | ... |

Run in the background so it keeps going if SSH drops:
```bash
nohup ./ingest_regulations.py CMS > cms_run.log 2>&1 &
```

Check progress:
```bash
tail -f cms_run.log
```

Check if it's still running:
```bash
ps aux | grep ingest
```

To run multiple agencies back to back:
```bash
for agency in CMS EPA FDA DOT; do
    ./ingest_regulations.py $agency >> all_runs.log 2>&1
done
```

---

## What happens if a document is already ingested?

Before downloading anything the script checks OpenSearch by document ID. If
the document is already there it logs `Already in OpenSearch, skipping` and
moves on. This means it is safe to re-run the script on the same agency or
have two people accidentally overlap — no duplicates will be created.

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
  Action: skipping this document — not ingested
============================================================
```

If you are getting blocked frequently, run fewer instances at once.

---

## S3 bucket structure expected

```
s3://mirrulations/raw-data/
  <agency>/                          e.g. CMS/
    <docket-id>/                     e.g. CMS-2026-1420/
      text-<docket-id>/
        docket/
          <docket-id>.json           e.g. CMS-2026-1420.json
```

---

## Input JSON format

Each docket JSON should contain document metadata including HTML file URLs:

```json
[
  {
    "docketId":   "CMS-2026-1420",
    "documentId": "CMS-2026-1420-0001",
    "fileFormats": [
      {
        "fileUrl": "https://downloads.regulations.gov/CMS-2026-1420-0001/content.html"
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
  "docketId":     "CMS-2026-1420",
  "documentId":   "CMS-2026-1420-0001",
  "documentText": "Full parsed plain text of the HTML document..."
}
```

The index is created automatically if it does not exist.

---

## Troubleshooting

**`connection refused` or can't reach OpenSearch**
Verify `OPENSEARCH_HOST` is the standard endpoint (not the FIPS one) and
that port 443 is open in your EC2 security group.

**`403 Forbidden` from OpenSearch**
Your IAM user is not in the `regulations-ingesters` group, or the OpenSearch
access policy hasn't been updated yet. Check with whoever manages the domain.

**`No dockets found for agency`**
The agency code is wrong or doesn't exist in the bucket. Check the exact
folder names in `s3://mirrulations/raw-data/` and use the folder name as `$1`.

**`Docket JSON not found`**
The expected JSON path doesn't exist for that docket. The docket is skipped
automatically and the script moves on to the next one.

**`Permissions 0644 for key file are too open`**
Run `chmod 400 your-key.pem` before SSH-ing in.