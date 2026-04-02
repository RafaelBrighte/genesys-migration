# Genesys → Zendesk Migration

Migrates Genesys call recordings stored in AWS S3 to Zendesk. For each recording, the script creates a closed Zendesk ticket with the conversation ID as the subject and attaches both the `.opus` audio file and `.opus_metadata.json` file.

## How it works

1. Scans the S3 bucket for `.opus` recordings (organised by `year/month/day/hour/conversation_id`)
2. Checks Zendesk to skip any recordings already migrated (safe to re-run)
3. Downloads the `.opus` and `.opus_metadata.json` files from S3
4. Uploads both as attachments to a new Zendesk ticket
5. Creates the ticket with status `closed` and the conversation ID as the subject

## Requirements

- Python 3.10+
- AWS IAM credentials with `AmazonS3ReadOnlyAccess`
- Zendesk API token

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/RafaelBrighte/genesys-migration.git
cd genesys-migration
```

**2. Install dependencies**
```bash
pip3 install requests boto3 python-dotenv
```

**3. Create a `.env` file**

```
GENESYS_CLIENT_ID=your-genesys-client-id
ZENDESK_EMAIL=support@yourcompany.com
ZENDESK_API_TOKEN=your-zendesk-api-token
ZENDESK_SUBDOMAIN=yoursubdomain
S3_BUCKET=your-s3-bucket-name
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-2
```

> **Important:** Use permanent IAM credentials (key starts with `AKIA`) for unattended runs. Temporary SSO credentials (`ASIA`) expire every few hours and will cause the script to fail mid-run.

## Usage

**Test with 5 recordings first**
```bash
python3 s3_to_zendesk.py --bulk-process --year 2025 --headless --limit 5
```

**Full migration for a specific year**
```bash
python3 s3_to_zendesk.py --bulk-process --year 2025 --headless
```

**Run overnight unattended on Linux/EC2**
```bash
nohup python3 s3_to_zendesk.py --bulk-process --year 2025 --headless --max-workers 2 > migration.log 2>&1 &
echo "PID: $!"
```

**Monitor progress**
```bash
tail -f migration.log
```

**Count tickets created so far**
```bash
grep "Successfully created ticket" migration.log | wc -l
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--bulk-process` | — | Process all recordings in the S3 bucket |
| `--year` | — | Filter by year (e.g. `--year 2025`) |
| `--headless` | — | Disable interactive prompts, required for unattended runs |
| `--max-workers` | 2 | Number of concurrent workers |
| `--limit` | — | Process only N recordings (useful for testing) |
| `--status` | `closed` | Zendesk ticket status (`new`, `open`, `pending`, `solved`, `closed`) |

## Running on EC2 (recommended for large migrations)

Running on a Linux EC2 instance is recommended — 175,000+ recordings can take 20+ hours, and the script runs entirely inside AWS making S3 downloads much faster.

```bash
# SSH into EC2
ssh -i your-key.pem ec2-user@your-ec2-ip

# Install dependencies
sudo yum install -y python3-pip
pip3 install requests boto3 python-dotenv

# Clone and configure
git clone https://github.com/RafaelBrighte/genesys-migration.git
cd genesys-migration
# Create .env file with your credentials

# Run in background — safe to disconnect after this
nohup python3 s3_to_zendesk.py --bulk-process --year 2025 --headless --max-workers 2 > migration.log 2>&1 &
```

## Re-running safely

The script checks Zendesk before processing each recording and skips any that already have a ticket. It is safe to stop and restart at any time — it will pick up where it left off without creating duplicates.

## S3 folder structure expected

```
{org-id}/
  year=2025/
    month=1/
      day=1/
        hour=0/
          conversation_id={id}/
            {file-uuid}.opus
            {file-uuid}.opus_metadata.json
```
