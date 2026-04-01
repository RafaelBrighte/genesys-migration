#!/usr/bin/env python3
"""
Zendesk Ticket Creator for Genesys Call Recordings

This script downloads call recordings from S3 and creates Zendesk tickets with the recording attached.
Each ticket's subject is the recording ID. Tickets are created as "solved" by default to avoid
creating thousands of open support requests.

Requirements:
- boto3
- requests
- python-dotenv

Environment variables required (loaded from .env file):
- GENESYS_CLIENT_ID
- ZENDESK_EMAIL
- ZENDESK_API_TOKEN
- ZENDESK_SUBDOMAIN
- S3_BUCKET

Usage:
# Process a single recording (local files)
python s3_to_zendesk.py --recording-id <recording_id> --local-file <path> --json-path <path>

# Process a single recording from S3
python s3_to_zendesk.py --recording-id <recording_id> --s3-key <s3_key>

# Bulk process all recordings in S3 bucket (recommended)
python s3_to_zendesk.py --bulk-process [--limit 10] [--status solved]

# Bulk processing with custom settings
python s3_to_zendesk.py --bulk-process --max-workers 10 --status closed --limit 100

Options:
--status: Set ticket status (new, open, pending, hold, solved, closed) - default: solved
--max-workers: Number of concurrent workers for bulk processing - default: 5
--limit: Limit number of recordings to process (useful for testing)
"""
import os
import sys
import argparse
import json

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded environment variables from .env file")
except ImportError:
    print("❌ python-dotenv not installed. Please run: pip3 install python-dotenv")
    sys.exit(127)
except Exception as e:
    print(f"❌ Error loading .env file: {e}")
    sys.exit(1)

import boto3
import requests
import tempfile
import logging
import subprocess
import time
import re
import signal
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, NoCredentialsError, TokenRetrievalError
from pathlib import Path

# Load environment variables
GENESYS_CLIENT_ID = os.getenv('GENESYS_CLIENT_ID')
ZENDESK_EMAIL = os.getenv('ZENDESK_EMAIL')
ZENDESK_API_TOKEN = os.getenv('ZENDESK_API_TOKEN')
ZENDESK_SUBDOMAIN = os.getenv('ZENDESK_SUBDOMAIN')
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_REGION = os.getenv('AWS_REGION', 'ap-southeast-2')

# Static IAM credentials (optional — preferred for unattended / overnight runs).
# When both are present the script uses them directly and never needs to prompt
# for an SSO re-login.  Set these in your .env file:
#   AWS_ACCESS_KEY_ID=AKIA...
#   AWS_SECRET_ACCESS_KEY=...
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')  # only needed for assumed-role/temporary creds

# Headless mode: no interactive prompts — any auth failure is a hard error.
# Enabled automatically when static IAM credentials are present, or explicitly
# via the --headless CLI flag (set later in main()).
HEADLESS_MODE: bool = bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)

# Validate required environment variables
required_vars = {
    'GENESYS_CLIENT_ID': GENESYS_CLIENT_ID,
    'ZENDESK_EMAIL': ZENDESK_EMAIL,
    'ZENDESK_API_TOKEN': ZENDESK_API_TOKEN,
    'ZENDESK_SUBDOMAIN': ZENDESK_SUBDOMAIN,
    'S3_BUCKET': S3_BUCKET
}

missing_vars = [var for var, value in required_vars.items() if not value]

if missing_vars:
    print("❌ Missing required environment variables:")
    for var in missing_vars:
        print(f"   - {var}")
    print("\nPlease check your .env file and ensure all variables are set.")
    sys.exit(1)

print("✅ All required environment variables loaded successfully")

ZENDESK_API_URL = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets.json"
ZENDESK_UPLOAD_URL = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/uploads.json"

# Ensure logs directory exists first
os.makedirs('logs', exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/s3_to_zendesk_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Global variables for credential management
CREDENTIAL_REFRESH_ENABLED = True
MAX_CREDENTIAL_RETRIES = 3

# AWS S3 client (will be refreshed automatically)
s3_client = None

def refresh_aws_clients():
    """Refresh the global S3 client.

    Credential priority:
    1. Static IAM credentials from .env  (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY)
       → never expire, safe for unattended/overnight runs.
    2. boto3.Session() with no explicit credentials
       → falls back to the standard provider chain (SSO cache, env vars, ~/.aws/…).
       Using Session() (not the top-level boto3.client shortcut) forces botocore
       to re-read the SSO token cache from disk on every call.
    """
    global s3_client
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,  # None is fine for IAM users
                region_name=AWS_REGION,
            )
            logger.debug("🔑 Using static IAM credentials from .env")
        else:
            session = boto3.Session(region_name=AWS_REGION)
            logger.debug("🔑 Using boto3 default credential chain (SSO / env / profile)")

        s3_client = session.client('s3')
        return True
    except Exception as e:
        logger.error(f"Failed to refresh AWS clients: {e}")
        return False

# Initialize AWS clients
refresh_aws_clients()

def validate_aws_credentials():
    """Check if AWS credentials are valid - auto-refresh if needed"""
    global s3_client
    
    for attempt in range(MAX_CREDENTIAL_RETRIES):
        try:
            sts_client = boto3.client('sts')
            response = sts_client.get_caller_identity()
            
            # Check if using temporary credentials (SSO)
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if hasattr(credentials, 'token') and credentials.token:
                logger.info("✅ AWS SSO credentials validated")
                logger.info(f"📋 Account ID: {response['Account']}")
                logger.info(f"👤 User/Role: {response['Arn']}")
                logger.info("🔄 Auto-refresh enabled for SSO credentials")
                return True
            else:
                logger.info("✅ AWS permanent credentials validated")
                logger.info(f"📋 Account ID: {response['Account']}")
                logger.info(f"👤 User/Role: {response['Arn']}")
                return True
                
        except (ClientError, NoCredentialsError, TokenRetrievalError) as e:
            logger.warning(f"⚠️  AWS credential validation failed (attempt {attempt + 1}/{MAX_CREDENTIAL_RETRIES}): {e}")
            
            if attempt < MAX_CREDENTIAL_RETRIES - 1:
                logger.info("🔄 Attempting to refresh credentials...")
                if wait_for_credential_refresh():
                    refresh_aws_clients()
                    continue
            
    logger.error("❌ Failed to validate AWS credentials after all retry attempts")
    return False

def wait_for_credential_refresh():
    """Attempt to refresh AWS credentials.

    Headless mode (static IAM creds in .env, or --headless flag):
        Static credentials never expire, so this should never be called.
        If it somehow is, log a clear error and return False immediately —
        no prompts, no blocking, safe to run in cron/launchd/screen.

    Interactive mode (SSO):
        1. Try boto3 auto-refresh — works silently if the SSO *session* is still
           valid but the short-lived temp credentials (1-hour lifetime) expired.
        2. Try `aws sso login` subprocess — opens a browser window once.
        3. Fall back to a manual "press Enter" prompt.
    """
    if not CREDENTIAL_REFRESH_ENABLED:
        return False

    # ── Headless path ──────────────────────────────────────────────────────────
    if HEADLESS_MODE:
        logger.error("❌ AWS credential error in headless mode.")
        logger.error("   Static IAM credentials should not expire.")
        logger.error("   Check that AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        logger.error("   in your .env file are correct and the IAM user is active.")
        return False

    # ── Interactive path ───────────────────────────────────────────────────────

    # Attempt 1: silent boto3 auto-refresh (works while SSO session is alive)
    logger.info("🔄 Attempting automatic credential refresh via new boto3 Session...")
    try:
        test_client = boto3.Session().client('sts', region_name=AWS_REGION)
        test_client.get_caller_identity()
        logger.info("✅ Credentials refreshed automatically (SSO token still valid)")
        refresh_aws_clients()
        return True
    except Exception:
        pass  # SSO session itself has expired — need user action

    # Attempt 2: `aws sso login` subprocess (opens browser)
    logger.info("🔄 Attempting `aws sso login` to renew the SSO session...")
    print("\n" + "="*60)
    print("🔐 AWS SSO TOKEN EXPIRED — opening browser for re-authentication")
    print("="*60)
    print("A browser window will open. Please log in, then return here.")
    print("(Ctrl+C to cancel)")
    print("="*60)
    try:
        result = subprocess.run(['aws', 'sso', 'login'], timeout=300)
        if result.returncode == 0:
            logger.info("✅ `aws sso login` succeeded — refreshing clients")
            refresh_aws_clients()
            return True
        else:
            logger.warning(f"⚠️  `aws sso login` exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        logger.warning("⚠️  `aws sso login` timed out after 5 minutes")
    except FileNotFoundError:
        logger.warning("⚠️  `aws` CLI not found — skipping subprocess refresh")
    except KeyboardInterrupt:
        print("\n🛑 Migration stopped by user")
        return False

    # Attempt 3: manual fallback
    print("\n" + "="*60)
    print("🔐 MANUAL CREDENTIAL REFRESH REQUIRED")
    print("="*60)
    print("Automatic refresh failed.  To continue the migration:")
    print("  1. Open a new terminal  →  run:  aws sso login")
    print("  2. Press Enter here once done")
    print("Or press Ctrl+C to stop.")
    print("="*60)
    try:
        input("Press Enter after refreshing your AWS SSO credentials...")
        refresh_aws_clients()
        return True
    except KeyboardInterrupt:
        print("\n🛑 Migration stopped by user")
        return False

def retry_with_credential_refresh(func):
    """Decorator to retry operations with credential refresh on failure"""
    def wrapper(*args, **kwargs):
        global s3_client
        
        for attempt in range(MAX_CREDENTIAL_RETRIES):
            try:
                return func(*args, **kwargs)
            except (ClientError, NoCredentialsError, TokenRetrievalError) as e:
                error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
                
                if error_code in ['ExpiredToken', 'TokenRefreshRequired', 'InvalidAccessKeyId'] or \
                   'expired' in str(e).lower() or 'invalid' in str(e).lower():
                    
                    logger.warning(f"🔄 Credential error detected (attempt {attempt + 1}/{MAX_CREDENTIAL_RETRIES}): {e}")
                    
                    if attempt < MAX_CREDENTIAL_RETRIES - 1:
                        if wait_for_credential_refresh():
                            refresh_aws_clients()
                            continue
                    
                    logger.error("❌ Failed to refresh credentials")
                    raise
                else:
                    # Non-credential related error, don't retry
                    raise
        
        return func(*args, **kwargs)
    return wrapper

def wait_for_credentials():
    """Wait for user to provide new credentials"""
    global s3_client
    
    print("\n" + "="*60)
    print("🔐 CREDENTIAL REFRESH REQUIRED")
    print("="*60)
    print("The migration has been paused due to expired AWS credentials.")
    print("Please provide new AWS credentials:")
    print("You can paste them as:")
    print("  1. Just the value: ASIAUDIVVKYNGXOJQMVN")
    print("  2. Export format: export AWS_ACCESS_KEY_ID=\"ASIAUDIVVKYNGXOJQMVN\"")
    print()
    
    def clean_credential_input(raw_input):
        """Clean credential input to extract just the value"""
        value = raw_input.strip()
        
        # Remove export prefix if present
        if value.startswith('export '):
            value = value[7:]  # Remove 'export '
        
        # Remove variable name if present (AWS_ACCESS_KEY_ID=...)
        if '=' in value:
            value = value.split('=', 1)[1]
        
        # Remove surrounding quotes
        value = value.strip('\'"')
        
        return value
    
    try:
        # Get credentials from user input
        print("AWS_ACCESS_KEY_ID:")
        access_key_raw = input("> ").strip()
        if not access_key_raw:
            print("❌ Access key cannot be empty.")
            return wait_for_credentials()
        access_key = clean_credential_input(access_key_raw)
            
        print("\nAWS_SECRET_ACCESS_KEY:")
        secret_key_raw = input("> ").strip()
        if not secret_key_raw:
            print("❌ Secret key cannot be empty.")
            return wait_for_credentials()
        secret_key = clean_credential_input(secret_key_raw)
            
        print("\nAWS_SESSION_TOKEN:")
        session_token_raw = input("> ").strip()
        if not session_token_raw:
            print("❌ Session token cannot be empty.")
            return wait_for_credentials()
        session_token = clean_credential_input(session_token_raw)
        
        # Set credentials in environment
        os.environ['AWS_ACCESS_KEY_ID'] = access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
        os.environ['AWS_SESSION_TOKEN'] = session_token
        
        print(f"\n🔍 Validating credentials (Access Key: {access_key[:8]}...)...")
        
        # Refresh the boto3 client with new credentials
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Check if credentials are now valid
        if validate_aws_credentials():
            print("✅ Credentials refreshed successfully!")
            return True
        else:
            print("❌ Credentials still invalid. Please try again.")
            return wait_for_credentials()  # Recursive retry
            
    except KeyboardInterrupt:
        print("\n❌ Migration cancelled by user.")
        return False
    except Exception as e:
        print(f"❌ Error setting credentials: {e}")
        return wait_for_credentials()

def print_progress_stats(completed, total, successful, failed, skipped, start_time):
    """Print detailed progress statistics"""
    elapsed = datetime.now() - start_time
    percentage = (completed / total) * 100 if total > 0 else 0
    success_rate = (successful / completed) * 100 if completed > 0 else 0
    
    # Calculate ETA
    if completed > 0:
        avg_time_per_record = elapsed.total_seconds() / completed
        remaining_records = total - completed
        eta_seconds = avg_time_per_record * remaining_records
        eta = timedelta(seconds=eta_seconds)
    else:
        eta = timedelta(seconds=0)
    
    print("\n" + "="*60)
    print("📊 MIGRATION PROGRESS")
    print("="*60)
    print(f"📁 Total recordings: {total}")
    print(f"✅ Completed: {completed} ({percentage:.1f}%)")
    print(f"🎯 Successful: {successful} ({success_rate:.1f}% success rate)")
    print(f"⏭️  Skipped (duplicates): {skipped}")
    print(f"❌ Failed: {failed}")
    print(f"⏰ Elapsed time: {str(elapsed).split('.')[0]}")
    print(f"🔮 Estimated time remaining: {str(eta).split('.')[0]}")
    print(f"⚡ Average: {elapsed.total_seconds()/completed:.1f} sec/record" if completed > 0 else "⚡ Average: calculating...")
    print("="*60)

def is_credential_error(error_msg):
    """Check if an error is related to credential expiration"""
    credential_errors = [
        'expired', 'expiredtoken', 'invalid', 'signatureodoesnotmatch',
        'invalidaccesskeyid', 'tokenrefreshrequired', 'credentials expired'
    ]
    return any(err in str(error_msg).lower() for err in credential_errors)

def get_latest_log_file():
    """Find the most recent S3 to Zendesk log file"""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        return None
    
    s3_logs = list(logs_dir.glob("s3_to_zendesk_*.log"))
    if not s3_logs:
        return None
    
    # Return most recent log file
    return max(s3_logs, key=lambda x: x.stat().st_mtime)

def extract_failed_recordings_from_log(log_file_path):
    """Extract failed recording IDs from the migration log for retry"""
    failed_recordings = []
    
    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                # Look for lines with "Failed to process recording"
                if "❌ Failed to process recording" in line:
                    # Extract recording ID using regex
                    match = re.search(r'Failed to process recording ([a-f0-9-]+):', line)
                    if match:
                        recording_id = match.group(1)
                        # Extract the error message too
                        error_start = line.find(': ') + 2
                        error_msg = line[error_start:].strip()
                        failed_recordings.append({
                            'recording_id': recording_id,
                            'error': error_msg,
                            'timestamp': line.split(' - ')[0]
                        })
                        
        logger.info(f"Found {len(failed_recordings)} failed recordings in log")
        return failed_recordings
        
    except Exception as e:
        logger.error(f"Error reading log file: {e}")
        return []

def find_recording_in_s3(recording_id, year_filter=None, month_filter=None):
    """Find a specific recording in S3 by its ID"""
    try:
        org_id = "600b8979-2932-4ce6-9b87-ba457b076e8f"
        
        # Search prefix based on filters or search everywhere
        if year_filter and month_filter:
            prefix = f"{org_id}/year={year_filter}/month={month_filter}/"
        elif year_filter:
            prefix = f"{org_id}/year={year_filter}/"
        else:
            prefix = f"{org_id}/"
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if recording_id in obj['Key'] and obj['Key'].endswith('.opus'):
                        return obj['Key']
        
        return None
        
    except Exception as e:
        logger.error(f"Error searching for recording {recording_id}: {e}")
        return None

def count_recordings_for_month(year, month):
    """Count recordings in S3 bucket for a specific month"""
    try:
        # Use S3 API to count recordings for the month
        org_id = "600b8979-2932-4ce6-9b87-ba457b076e8f"
        # Use single digit for months 1-9, no zero padding
        prefix = f"{org_id}/year={year}/month={month}/"
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
        
        count = 0
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.opus'):
                        count += 1
        
        return count
        
    except Exception as e:
        logger.error(f"Error counting recordings for {year}-{month}: {e}")
        return 0

def process_single_recording_by_id(recording_id, month, ticket_status="solved"):
    """Process a single recording by ID and month"""
    try:
        # Parse month to get year and month components
        year, month_num = month.split('-')
        
        # Find the recording in S3
        opus_key = find_recording_in_s3(recording_id, year, month_num)
        if not opus_key:
            return {
                'success': False,
                'error': f"Recording {recording_id} not found in S3"
            }
        
        # Look for metadata file
        metadata_key = opus_key.replace('.opus', '.opus_metadata.json')
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=metadata_key)
            metadata_exists = True
        except:
            metadata_exists = False
            metadata_key = None
        
        # Create recording info structure
        recording_info = {
            'recording_id': recording_id,
            'opus_key': opus_key,
            'metadata_key': metadata_key if metadata_exists else None
        }
        
        # Process the recording
        result = process_single_recording(recording_info, ticket_status)
        return result
        
    except Exception as e:
        logger.error(f"Error processing recording {recording_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }
        # Use S3 API to count recordings for the month
        org_id = "600b8979-2932-4ce6-9b87-ba457b076e8f"
        # Use single digit for months 1-9, no zero padding
        prefix = f"{org_id}/year={year}/month={month}/"
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
        
        count = 0
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.opus'):
                        count += 1
        
        return count
        
    except Exception as e:
        logger.error(f"Error counting recordings for {year}-{month}: {e}")
        return 0

def parse_log_stats(log_file):
    """Parse log file for progress statistics"""
    if not log_file or not log_file.exists():
        return None
    
    stats = {
        'total_found': 0,
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'start_time': None,
        'last_update': None,
        'errors': [],
        'credential_warnings': 0,
        'tickets_created': []
    }
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                # Parse timestamps
                timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if timestamp_match and not stats['start_time']:
                    stats['start_time'] = timestamp_match.group(1)
                if timestamp_match:
                    stats['last_update'] = timestamp_match.group(1)
                
                # Parse progress indicators
                if 'Found' in line and '.opus recordings' in line:
                    match = re.search(r'Found (\d+) \.opus recordings', line)
                    if match:
                        stats['total_found'] = int(match.group(1))
                
                if 'Progress:' in line:
                    # Progress: 5/100 (5.0%) - 4 successful
                    match = re.search(r'Progress: (\d+)/(\d+) \(.*?\) - (\d+) successful', line)
                    if match:
                        stats['processed'] = int(match.group(1))
                        stats['successful'] = int(match.group(3))
                        stats['failed'] = stats['processed'] - stats['successful']
                
                if 'Successfully created ticket' in line:
                    ticket_match = re.search(r'ticket (\d+) for recording ([a-f0-9-]+)', line)
                    if ticket_match:
                        stats['tickets_created'].append({
                            'ticket_id': ticket_match.group(1),
                            'recording_id': ticket_match.group(2)
                        })
                
                # Track errors
                if '❌' in line or 'ERROR' in line:
                    stats['errors'].append(line.strip())
                
                # Track credential warnings
                if 'credential' in line.lower() and ('expire' in line.lower() or 'invalid' in line.lower()):
                    stats['credential_warnings'] += 1
                    
    except Exception as e:
        logger.error(f"Error parsing log file: {e}")
        return None
    
    return stats

def display_stats(stats):
    """Display formatted statistics"""
    if not stats:
        logger.error("❌ No statistics available")
        return
    
    logger.info("="*60)
    logger.info("📊 MIGRATION PROGRESS SUMMARY")
    logger.info("="*60)
    
    if stats['start_time']:
        logger.info(f"🕐 Started: {stats['start_time']}")
    if stats['last_update']:
        logger.info(f"🔄 Last Update: {stats['last_update']}")
    
    logger.info(f"📁 Total Recordings Found: {stats['total_found']:,}")
    logger.info(f"⚙️  Processed: {stats['processed']:,}")
    logger.info(f"✅ Successful: {stats['successful']:,}")
    logger.info(f"❌ Failed: {stats['failed']:,}")
    
    if stats['processed'] > 0:
        success_rate = (stats['successful'] / stats['processed']) * 100
        logger.info(f"📈 Success Rate: {success_rate:.1f}%")
        
        if stats['total_found'] > 0:
            progress = (stats['processed'] / stats['total_found']) * 100
            logger.info(f"🎯 Overall Progress: {progress:.1f}%")
            
            if stats['processed'] > 0:
                # Estimate remaining time
                if stats['start_time'] and stats['last_update']:
                    try:
                        start = datetime.strptime(stats['start_time'], "%Y-%m-%d %H:%M:%S")
                        last = datetime.strptime(stats['last_update'], "%Y-%m-%d %H:%M:%S")
                        elapsed = (last - start).total_seconds()
                        rate = stats['processed'] / elapsed if elapsed > 0 else 0
                        remaining = stats['total_found'] - stats['processed']
                        eta_seconds = remaining / rate if rate > 0 else 0
                        eta = datetime.now() + timedelta(seconds=eta_seconds)
                        logger.info(f"⏱️  Estimated Completion: {eta.strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info(f"📊 Processing Rate: {rate:.1f} recordings/second")
                    except:
                        pass
    
    logger.info(f"🎫 Tickets Created: {len(stats['tickets_created'])}")
    if stats['credential_warnings'] > 0:
        logger.info(f"⚠️  Credential Warnings: {stats['credential_warnings']}")
    if stats['errors']:
        logger.info(f"❌ Errors: {len(stats['errors'])}")
    
    logger.info("="*60)


def is_credential_error(error):
    """Check if the error is related to AWS credentials"""
    error_str = str(error).lower()
    credential_indicators = [
        'nocredentialserror',
        'invalidaccesskeyid',
        'signaturedoesnotmatch', 
        'tokenrefreshrequired',
        'expiredtoken',
        'credentialsnotfound',
        'unable to locate credentials',
        'the security token included in the request is invalid',
        'the security token included in the request is expired',
        'invalid security token',
        'credentials have expired',
        'aws credentials not found',
        'no credentials found'
    ]
    return any(indicator in error_str for indicator in credential_indicators)


def download_recording(s3_key, local_path, retry_on_auth_error=True):
    """Download recording from S3 with automatic credential refresh on auth errors"""
    global s3_client
    
    for attempt in range(MAX_CREDENTIAL_RETRIES):
        try:
            s3_client.download_file(S3_BUCKET, s3_key, local_path)
            return  # Success
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if retry_on_auth_error and error_code in ['ExpiredToken', 'InvalidAccessKeyId', 'SignatureDoesNotMatch', 'TokenRefreshRequired']:
                logger.warning(f"⚠️  AWS credential error ({error_code}) - attempt {attempt + 1}/{MAX_CREDENTIAL_RETRIES}")
                
                if attempt < MAX_CREDENTIAL_RETRIES - 1:
                    logger.info("🔄 Refreshing AWS credentials...")
                    if wait_for_credential_refresh():
                        refresh_aws_clients()
                        continue
                    else:
                        logger.error("❌ User cancelled credential refresh")
                        raise
                else:
                    logger.error("❌ Failed to refresh credentials after all attempts")
                    raise
            else:
                # Non-credential error, don't retry
                raise

def list_all_recordings(year_filter=None, month_filter=None):
    """List all .opus recordings in the S3 bucket, optionally filtered by year/month"""
    global s3_client
    recordings = []
    
    # Build prefix for filtering
    org_id = "600b8979-2932-4ce6-9b87-ba457b076e8f"
    # Simple approach - scan the 2025 folder directly
    if year_filter == 2025:
        prefix = f"{org_id}/year=2025/"
        logger.info(f"📂 Directly scanning 2025 folder: {prefix}")
    else:
        prefix = f"{org_id}/"
        
        if year_filter:
            prefix += f"year={year_filter}/"
            if month_filter:
                # Use single digit for months, no zero padding to match S3 structure
                prefix += f"month={month_filter}/"
    
    logger.info(f"Scanning S3 bucket {S3_BUCKET} for .opus recordings...")
    if year_filter and month_filter:
        logger.info(f"🗓️  Filtering by: {year_filter}-{month_filter:02d}")
        logger.info(f"📂 Using prefix: {prefix}")
    elif year_filter == 2025:
        logger.info(f"🗓️  Scanning 2025 folder directly")
        logger.info(f"📂 Using prefix: {prefix}")
    
    # Wrap the entire scan (both paginator setup AND iteration) in a credential
    # retry loop.  Previously only the paginator creation was protected, so an
    # expired token mid-iteration would crash the script silently.
    CREDENTIAL_ERROR_CODES = {
        'ExpiredToken', 'InvalidAccessKeyId', 'SignatureDoesNotMatch', 'TokenRefreshRequired'
    }
    continuation_token = None

    while True:
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            paginate_kwargs = {'Bucket': S3_BUCKET, 'Prefix': prefix}
            if continuation_token:
                # Resume from where we left off after a credential refresh
                paginate_kwargs['PaginationConfig'] = {'StartingToken': continuation_token}
            page_iterator = paginator.paginate(**paginate_kwargs)

            for page in page_iterator:
                # Track the token so we can resume after a mid-scan credential error
                continuation_token = page.get('NextContinuationToken')

                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.opus'):
                            # Extract conversation_id from path as the unique recording ID
                            # Path format: .../conversation_id=ABC123/filename.opus
                            if 'conversation_id=' in key:
                                conversation_id = key.split('conversation_id=')[1].split('/')[0]
                                recording_id = conversation_id
                            else:
                                # Fallback to filename if conversation_id not found
                                filename = key.split('/')[-1]
                                recording_id = filename.replace('.opus', '')
                                logger.warning(f"No conversation_id found in path {key}, using filename: {recording_id}")

                            # Set candidate metadata key — existence is checked during processing,
                            # not here, to avoid thousands of extra S3 API calls during the scan.
                            metadata_key = key.replace('.opus', '.json')

                            recordings.append({
                                'recording_id': recording_id,
                                'opus_key': key,
                                'metadata_key': metadata_key,
                                'has_metadata': False,  # resolved during processing
                                'size': obj['Size'],
                                'last_modified': obj['LastModified']
                            })

            break  # scan completed successfully — exit the retry loop

        except (ClientError, NoCredentialsError, TokenRetrievalError) as e:
            error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '') if hasattr(e, 'response') else ''
            if error_code in CREDENTIAL_ERROR_CODES or is_credential_error(e):
                logger.warning(f"⚠️  Credential error during S3 scan ({error_code or e}) — attempting refresh and resuming from continuation token")
                if wait_for_credential_refresh():
                    refresh_aws_clients()
                    # Loop back and restart the paginator from continuation_token
                    continue
                else:
                    logger.error("❌ Could not refresh credentials — aborting scan")
                    raise
            else:
                raise

    logger.info(f"Found {len(recordings)} .opus recordings in bucket")
    return recordings

def upload_attachment_to_zendesk(file_path, filename):
    with open(file_path, 'rb') as f:
        auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
        files = {'file': (filename, f)}
        params = {'filename': filename}
        response = requests.post(ZENDESK_UPLOAD_URL, auth=auth, files=files, params=params)
        response.raise_for_status()
        return response.json()['upload']['token']

def process_single_recording(recording_info, ticket_status="solved"):
    """Process a single recording and create a Zendesk ticket"""
    recording_id = recording_info['recording_id']
    opus_key = recording_info['opus_key']
    metadata_key = recording_info['metadata_key']
    
    try:
        logger.info(f"Processing recording {recording_id}...")

        # Check for an existing ticket BEFORE doing any expensive S3/Zendesk work
        logger.info(f"Checking for existing tickets for {recording_id}...")
        exists, existing_ticket_ids = check_ticket_exists(recording_id)
        if exists:
            logger.info(f"⏭️  Skipping {recording_id} - already has ticket(s): {existing_ticket_ids}")
            return {
                'recording_id': recording_id,
                'ticket_id': existing_ticket_ids[0],
                'status': 'already_exists',
                'existing_tickets': existing_ticket_ids
            }

        # ── Download .opus recording ──────────────────────────────────────────
        with tempfile.NamedTemporaryFile(suffix='.opus', delete=False) as tmp_opus:
            logger.info(f"Downloading recording {opus_key}...")
            download_recording(opus_key, tmp_opus.name)
            opus_path = tmp_opus.name

        # ── Download JSON metadata (best-effort) ──────────────────────────────
        metadata_path = None
        if metadata_key:
            try:
                with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_json:
                    logger.info(f"Downloading metadata {metadata_key}...")
                    download_recording(metadata_key, tmp_json.name)
                    metadata_path = tmp_json.name
                logger.info(f"✅ Metadata downloaded for {recording_id}")
            except Exception as e:
                logger.warning(f"⚠️  No metadata file for {recording_id}: {e}")
                metadata_path = None

        # ── Upload both files to Zendesk as attachments ───────────────────────
        upload_tokens = []

        logger.info(f"Uploading {recording_id}.opus to Zendesk...")
        upload_tokens.append(upload_attachment_to_zendesk(opus_path, f"{recording_id}.opus"))

        if metadata_path:
            logger.info(f"Uploading {recording_id}.json to Zendesk...")
            upload_tokens.append(upload_attachment_to_zendesk(metadata_path, f"{recording_id}.json"))

        # ── Create ticket ─────────────────────────────────────────────────────
        has_meta = "recording + metadata" if metadata_path else "recording only"
        comment = f"Genesys call recording for conversation {recording_id}.\nAttachments: {has_meta}."

        logger.info(f"Creating Zendesk ticket for conversation {recording_id}...")
        ticket_id = create_ticket_with_attachment(recording_id, upload_tokens, comment, ticket_status)

        # ── Cleanup temp files ────────────────────────────────────────────────
        try:
            os.unlink(opus_path)
            if metadata_path:
                os.unlink(metadata_path)
        except Exception as e:
            logger.warning(f"Could not clean up temp files: {e}")
        
        logger.info(f"✅ Successfully created ticket {ticket_id} for recording {recording_id}")
        return {
            'recording_id': recording_id,
            'ticket_id': ticket_id,
            'status': 'success'
        }
        
    except Exception as e:
        # Check if this is a credential error
        if is_credential_error(e):
            logger.error(f"🔑 Credential error for recording {recording_id}: {e}")
            return {
                'recording_id': recording_id,
                'ticket_id': None,
                'status': 'credential_error',
                'error': str(e)
            }
        else:
            logger.error(f"❌ Failed to process recording {recording_id}: {e}")
            return {
                'recording_id': recording_id,
                'ticket_id': None,
                'status': 'failed',
                'error': str(e)
            }

def check_ticket_exists(recording_id):
    """Check if a ticket already exists for this recording ID"""
    try:
        auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
        search_url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/search.json"
        
        # Subject is exactly the conversation ID (no prefix)
        params = {
            'query': f'subject:"{recording_id}" type:ticket'
        }
        
        response = requests.get(search_url, auth=auth, params=params)
        response.raise_for_status()
        
        results = response.json()
        if results['count'] > 0:
            existing_tickets = [result['id'] for result in results['results']]
            logger.warning(f"🔄 Recording {recording_id} already has {results['count']} ticket(s): {existing_tickets}")
            return True, existing_tickets
        
        return False, []
        
    except Exception as e:
        logger.warning(f"⚠️  Could not check for existing tickets for {recording_id}: {e}")
        return False, []


def create_ticket_with_attachment(recording_id, upload_token, comment=None, status="closed"):
    """Create a Zendesk ticket whose subject is the conversation ID.

    Both the .opus recording and the .json metadata are passed as upload tokens
    so they appear as attachments on the ticket's first comment.
    """
    upload_tokens = upload_token if upload_token is not None else []
    auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
    headers = {'Content-Type': 'application/json'}

    body = comment or "Genesys call recording attached."

    data = {
        "ticket": {
            # Subject is exactly the conversation ID — no prefix
            "subject": recording_id,
            "status": status,
            "comment": {
                "body": body,
                "uploads": upload_tokens  # both .opus and .json tokens land here
            }
        }
    }
    response = requests.post(ZENDESK_API_URL, auth=auth, headers=headers, json=data)
    response.raise_for_status()
    return response.json()['ticket']['id']

def main():
    parser = argparse.ArgumentParser(description="Send S3 or local call recording to Zendesk as ticket attachment.")
    parser.add_argument('--recording-id', required=False, help='Recording ID (used as ticket subject)')
    parser.add_argument('--s3-key', required=False, help='S3 key of the recording file (if not standard)')
    parser.add_argument('--json-path', required=False, help='Path to local JSON metadata file')
    parser.add_argument('--local-file', required=False, help='Path to local .opus file (bypass S3 download)')
    parser.add_argument('--bulk-process', action='store_true', help='Process all recordings in S3 bucket')
    parser.add_argument('--year', type=int, help='Filter by specific year (for bulk processing)')
    parser.add_argument('--month', type=str, help='Filter by specific month in YYYY-MM format (for bulk processing, e.g., 2025-10)')
    parser.add_argument('--status', default='closed', choices=['new', 'open', 'pending', 'hold', 'solved', 'closed'], 
                       help='Ticket status (default: closed)')
    parser.add_argument('--max-workers', type=int, default=5, help='Number of concurrent workers for bulk processing (default: 5)')
    parser.add_argument('--limit', type=int, help='Limit number of recordings to process (for testing)')
    parser.add_argument('--auto-restart', action='store_true', help='Automatically pause and wait for credential refresh on credential errors')
    parser.add_argument('--headless', action='store_true',
                        help='Disable all interactive prompts — safe for cron/launchd/overnight runs. '
                             'Any credential failure becomes a hard error. '
                             'Requires AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in .env.')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed recordings from latest log file')
    parser.add_argument('--retry-log', type=str, help='Specific log file to retry failed recordings from')
    parser.add_argument('--show-stats', action='store_true', help='Show migration statistics from latest log')
    parser.add_argument('--count-month', type=str, help='Count recordings for specific month (YYYY-MM format)')
    parser.add_argument('--monitor', action='store_true', help='Monitor migration progress in real-time')
    parser.add_argument('--renew-credentials', action='store_true', help='Interactively renew AWS credentials')
    args = parser.parse_args()

    # Activate headless mode if the flag was passed explicitly
    # (it may already be True if static creds were detected in the .env)
    global HEADLESS_MODE
    if args.headless:
        HEADLESS_MODE = True
        if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
            logger.error("❌ --headless requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
            logger.error("   See .env.headless for the required format.")
            sys.exit(1)
        logger.info("🤖 Headless mode enabled — static IAM credentials will be used, no interactive prompts")

    # Check for credential renewal first
    if args.renew_credentials:
        if wait_for_credentials():
            logger.info("✅ Credentials renewed successfully!")
            logger.info("💡 Now you can run the script with your desired parameters.")
        else:
            logger.error("❌ Failed to renew credentials")
            sys.exit(1)
        return
    
    # Check for monitoring/utility operations first
    if args.show_stats:
        log_file = get_latest_log_file()
        if not log_file:
            logger.error("No log files found")
            sys.exit(1)
        
        # Simple stats display without import
        stats = parse_log_stats(log_file)
        if stats:
            display_stats(stats)
        else:
            logger.error("No statistics available")
        return
    
    if args.count_month:
        try:
            year_str, month_str = args.count_month.split('-')
            year, month = int(year_str), int(month_str)
        except ValueError:
            logger.error(f"Invalid month format: {args.count_month}. Use YYYY-MM format")
            sys.exit(1)
        
        if not validate_aws_credentials():
            logger.error("❌ Cannot proceed without valid AWS credentials")
            sys.exit(1)
        
        count = count_recordings_for_month(year, month)
        logger.info(f"📊 Found {count:,} recordings for {year}-{month:02d}")
        return
    
    if args.retry_failed or args.retry_log:
        # Retry failed recordings
        log_file = args.retry_log if args.retry_log else get_latest_log_file()
        if not log_file:
            logger.error("No log file found for retry")
            sys.exit(1)
            
        logger.info(f"🔄 Retrying failed recordings from: {log_file}")
        
        if not validate_aws_credentials():
            if args.auto_restart and wait_for_credentials():
                logger.info("✅ Credentials refreshed")
            else:
                logger.error("❌ Cannot proceed without valid AWS credentials")
                sys.exit(1)
        
        failed_recordings = extract_failed_recordings_from_log(log_file)
        if not failed_recordings:
            logger.info("✅ No failed recordings found to retry")
            return
        
        failed_ids = [r['recording_id'] for r in failed_recordings]
        logger.info(f"🎯 Found {len(failed_ids)} failed recordings to retry")
        
        # Process failed recordings
        successful = 0
        failed = 0
        
        for i, recording_id in enumerate(failed_ids, 1):
            logger.info(f"🔄 Retrying {i}/{len(failed_ids)}: {recording_id}")
            
            # Find in S3 (try current month first, then search broadly)
            s3_key = find_recording_in_s3(recording_id, args.year, 
                                        int(args.month.split('-')[1]) if args.month else None)
            
            if not s3_key:
                # Try broader search
                s3_key = find_recording_in_s3(recording_id)
            
            if not s3_key:
                logger.warning(f"❓ Recording {recording_id} not found in S3")
                failed += 1
                continue
            
            # Process the recording
            try:
                recording_info = {
                    'recording_id': recording_id,
                    'opus_key': s3_key,
                    'metadata_key': s3_key.replace('.opus', '.json'),
                    'has_metadata': True,
                    'size': 0,
                    'last_modified': datetime.now()
                }
                
                result = process_single_recording(recording_info, args.status)
                if result['status'] == 'success':
                    logger.info(f"✅ Successfully retried {recording_id}")
                    successful += 1
                else:
                    logger.error(f"❌ Retry failed for {recording_id}: {result.get('error', 'Unknown')}")
                    failed += 1
                    
            except Exception as e:
                logger.error(f"❌ Exception during retry of {recording_id}: {e}")
                failed += 1
        
        # Retry summary
        logger.info("=" * 60)
        logger.info("🎯 RETRY COMPLETE")
        logger.info("=" * 60)
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"📊 Success rate: {successful/(successful+failed)*100:.1f}%")
        return
    
    if not args.recording_id and not args.bulk_process:
        parser.error('Either --recording-id, --bulk-process, or a utility option must be specified')
    
    # Validate AWS credentials before starting any operation
    if not validate_aws_credentials():
        if args.auto_restart:
            logger.warning("❌ Invalid AWS credentials detected")
            if not wait_for_credentials():
                logger.error("❌ Cannot proceed without valid AWS credentials")
                sys.exit(1)
        else:
            logger.error("❌ Cannot proceed without valid AWS credentials")
            logger.error("💡 Please refresh your AWS credentials and try again")
            logger.error("💡 Or use --auto-restart flag for automatic credential refresh prompts")
            sys.exit(1)
    
    if args.bulk_process:
        # Bulk processing mode
        year_filter = None
        month_filter = None
        
        # Parse month argument if provided (YYYY-MM format)
        if args.month:
            try:
                year_str, month_str = args.month.split('-')
                year_filter = int(year_str)
                month_filter = int(month_str)
            except ValueError:
                logger.error(f"Invalid month format: {args.month}. Use YYYY-MM format (e.g., 2025-10)")
                sys.exit(1)
        elif args.year:
            year_filter = args.year
        
        filter_desc = ""
        if year_filter and month_filter:
            filter_desc = f" for {year_filter}-{month_filter:02d}"
        elif year_filter:
            filter_desc = f" for year {year_filter}"
        
        logger.info(f"Starting bulk processing of recordings in S3 bucket{filter_desc}...")
        recordings = list_all_recordings(year_filter, month_filter)
        
        if args.limit:
            recordings = recordings[:args.limit]
            logger.info(f"Limited to first {args.limit} recordings for testing")
        
        if not recordings:
            logger.error("No .opus recordings found in S3 bucket")
            sys.exit(1)
        
        logger.info(f"Processing {len(recordings)} recordings with {args.max_workers} workers...")
        
        # Process recordings concurrently with monitoring
        results = []
        successful = 0
        failed = 0
        skipped = 0  # New counter for already existing tickets
        start_time = datetime.now()
        last_credential_check = start_time
        last_stats_update = start_time
        credential_check_interval = timedelta(minutes=10)  # Check credentials every 10 minutes
        stats_interval = timedelta(seconds=30)  # Update stats every 30 seconds
        
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            future_to_recording = {
                executor.submit(process_single_recording, recording, args.status): recording
                for recording in recordings
            }
            
            for future in as_completed(future_to_recording):
                result = future.result()
                results.append(result)
                
                # Update counters
                if result['status'] == 'success':
                    successful += 1
                elif result['status'] == 'already_exists':
                    skipped += 1
                    logger.debug(f"⏭️  Skipped duplicate: {result['recording_id']}")
                else:
                    failed += 1
                
                # Check for credential errors - STOP processing on credential failure
                if result['status'] == 'credential_error':
                    logger.error("🔑 CREDENTIAL ERROR DETECTED - STOPPING PROCESSING")
                    logger.error("=" * 60)
                    logger.error(f"❌ Failed recording: {result['recording_id']}")
                    logger.error(f"❌ Error: {result['error']}")
                    logger.error("=" * 60)
                    logger.error("🛑 Script stopped due to credential failure.")
                    logger.error("💡 To continue:")
                    logger.error("   1. Renew your AWS credentials")
                    logger.error("   2. Run: python3 s3_to_zendesk.py --renew-credentials")
                    logger.error("   3. Restart the script with the same parameters")
                    logger.error("=" * 60)
                    
                    # Cancel all remaining futures
                    for remaining_future in future_to_recording:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    
                    # Save current progress before exiting
                    results_file = f"logs/bulk_processing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}_STOPPED.json"
                    os.makedirs("logs", exist_ok=True)
                    with open(results_file, 'w') as f:
                        json.dump({
                            'timestamp': datetime.now().isoformat(),
                            'filter': args.month or f"year-{args.year}" if args.year else "all",
                            'total_planned': len(recordings),
                            'processed': len(results),
                            'successful': successful,
                            'failed': failed,
                            'stopped_due_to_credentials': True,
                            'results': results
                        }, f, indent=2)
                    
                    logger.info(f"📄 Progress saved to: {results_file}")
                    sys.exit(1)
                
                # Periodic stats display
                now = datetime.now()
                if now - last_stats_update > stats_interval:
                    print_progress_stats(len(results), len(recordings), successful, failed, skipped, start_time)
                    last_stats_update = now
                
                # Periodic credential validation for long-running operations
                if now - last_credential_check > credential_check_interval:
                    logger.info("🔍 Periodic credential validation...")
                    if not validate_aws_credentials():
                        logger.error("🔑 CREDENTIALS EXPIRED DURING PROCESSING - STOPPING")
                        logger.error("=" * 60)
                        logger.error("🛑 Script stopped due to credential expiration.")
                        logger.error("💡 To continue:")
                        logger.error("   1. Renew your AWS credentials")
                        logger.error("   2. Run: python3 s3_to_zendesk.py --renew-credentials")
                        logger.error("   3. Restart the script with the same parameters")
                        logger.error("=" * 60)
                        
                        # Cancel all remaining futures
                        for remaining_future in future_to_recording:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        
                        # Save current progress before exiting
                        results_file = f"logs/bulk_processing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}_EXPIRED.json"
                        os.makedirs("logs", exist_ok=True)
                        with open(results_file, 'w') as f:
                            json.dump({
                                'timestamp': datetime.now().isoformat(),
                                'filter': args.month or f"year-{args.year}" if args.year else "all",
                                'total_planned': len(recordings),
                                'processed': len(results),
                                'successful': successful,
                                'failed': failed,
                                'stopped_due_to_credentials': True,
                                'results': results
                            }, f, indent=2)
                        
                        logger.info(f"📄 Progress saved to: {results_file}")
                        sys.exit(1)
                    last_credential_check = now
        
        # Final progress display
        print_progress_stats(len(results), len(recordings), successful, failed, skipped, start_time)
        
        # Summary
        credential_errors = [r for r in results if r['status'] == 'credential_error']
        other_failed = [r for r in results if r['status'] == 'failed']
        already_exists = [r for r in results if r['status'] == 'already_exists']
        
        logger.info("="*60)
        logger.info("🏁 BULK PROCESSING COMPLETE")
        logger.info("="*60)
        logger.info(f"📊 Total recordings processed: {len(results)}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"⏭️  Skipped (already exist): {len(already_exists)}")
        logger.info(f"🔑 Credential errors: {len(credential_errors)}")
        logger.info(f"❌ Other failures: {len(other_failed)}")
        actual_processing_rate = successful/(len(results) - len(already_exists))*100 if (len(results) - len(already_exists)) > 0 else 0
        logger.info(f"🎯 Success rate (new recordings): {actual_processing_rate:.1f}%")
        
        if already_exists:
            logger.info(f"⏭️  {len(already_exists)} recordings were skipped because tickets already exist")
            if len(already_exists) <= 10:  # Show details if not too many
                for result in already_exists[:10]:
                    logger.info(f"  - {result['recording_id']} (tickets: {result.get('existing_tickets', [])})")
        
        if credential_errors:
            logger.warning("🔑 Recordings failed due to credential errors (retry after refreshing credentials):")
            for result in credential_errors:
                logger.warning(f"  - {result['recording_id']}")
        
        if other_failed:
            logger.error("❌ Recordings failed with other errors:")
            for result in other_failed:
                logger.error(f"  - {result['recording_id']}: {result.get('error', 'Unknown error')}")
        
        # Save results to file
        results_file = f"logs/bulk_processing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"📄 Detailed results saved to: {results_file}")
        
        # Display retry suggestion for credential errors
        if credential_errors:
            logger.info("\n💡 To retry credential-failed recordings, refresh your AWS credentials and run:")
            logger.info(f"   python s3_to_zendesk.py --bulk-process --month {args.month if args.month else 'YYYY-MM'} --auto-restart")
        
    else:
        # Single recording mode
        recording_id = args.recording_id
        # Use local file if provided, else download from S3
        if args.local_file:
            local_file = os.path.expanduser(args.local_file)
            if not os.path.exists(local_file):
                print(f"Local file not found: {local_file}")
                sys.exit(1)
        else:
            s3_key = args.s3_key or f"{recording_id}.opus"
            local_file = f"/tmp/{recording_id}.opus"
            print(f"Downloading recording from S3: {s3_key}")
            download_recording(s3_key, local_file)

        # Determine JSON metadata file path
        json_path = args.json_path or os.path.expanduser(f"~/Downloads/{recording_id}.opus_metadata.json")
        if not os.path.exists(json_path):
            print(f"Warning: JSON metadata file not found at {json_path}. Using default comment.")
            comment = None
        else:
            with open(json_path, 'r') as jf:
                try:
                    metadata = json.load(jf)
                    comment = json.dumps(metadata, indent=2)
                except Exception as e:
                    print(f"Error reading JSON metadata: {e}. Using default comment.")
                    comment = None

        print("Uploading recording to Zendesk...")
        upload_tokens = []
        upload_tokens.append(upload_attachment_to_zendesk(local_file, f"{recording_id}.opus"))
        if os.path.exists(json_path):
            upload_tokens.append(upload_attachment_to_zendesk(json_path, f"{recording_id}.opus_metadata.json"))

        print("Creating Zendesk ticket...")
        ticket_id = create_ticket_with_attachment(recording_id, upload_tokens, comment, args.status)
        print(f"Zendesk ticket created: {ticket_id}")
        # Only remove temp file if it was downloaded from S3
        if not args.local_file and os.path.exists(local_file):
            os.remove(local_file)

if __name__ == "__main__":
    main()
