#!/usr/bin/env python3
"""Retry specific failed recordings directly without scanning all of S3."""

import os, sys, boto3, tempfile, logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add current directory to path so we can import from s3_to_zendesk
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from s3_to_zendesk import process_single_recording, refresh_aws_clients

FAILED_IDS = [
    "0be1fce7-9656-420f-8e5c-bd0b0f9ab948",
    "0d14a4b2-c62d-4ce8-b5fe-046b54aa05c4",
    "20600b79-2bf1-4eaa-9e43-2ffe054e69cc",
    "22eca51b-f0eb-4966-8c07-5cb33ac8e979",
    "64d5c847-30af-4de9-a52a-cab452f1c429",
    "89ecf173-88d0-4f5d-93c6-cd5140eaad03",
    "8a0618c5-6c16-469e-a592-c0b4c2ba3155",
    "98e80447-17ea-4610-b2b0-d31b0044d2eb",
    "9f605c40-d545-4145-a5d5-a3a8ffc37812",
    "afb6eb24-e1df-49ce-a6f2-959989ff4897",
    "b79ec6e1-da2b-4e98-9023-92d345e84570",
    "ca82db06-9282-45dd-ae32-94317db76bf5",
    "e414452f-6323-4ed2-9a4b-e760680f9c06",
    "eb490b01-e0ac-487a-9a86-147999ba752d",
    "ef8b325c-d561-4573-ac5a-565d01a39d3b",
]

PREFIX = "600b8979-2932-4ce6-9b87-ba457b076e8f"
BUCKET = os.getenv('S3_BUCKET')

def find_opus_key(s3_client, conv_id):
    """Search S3 for the opus file for a given conversation ID."""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/"):
        for obj in page.get('Contents', []):
            if conv_id in obj['Key'] and obj['Key'].endswith('.opus'):
                return obj['Key']
    return None

def main():
    refresh_aws_clients()

    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'ap-southeast-2')
    )

    successful = 0
    failed = 0

    for i, conv_id in enumerate(FAILED_IDS, 1):
        logger.info(f"[{i}/{len(FAILED_IDS)}] Looking up {conv_id}...")
        opus_key = find_opus_key(s3, conv_id)

        if not opus_key:
            logger.warning(f"  ⚠️  Not found in S3: {conv_id}")
            failed += 1
            continue

        result = process_single_recording({
            'recording_id': conv_id,
            'opus_key': opus_key,
            'metadata_key': opus_key.replace('.opus', '.opus_metadata.json')
        }, ticket_status='closed')

        status = result.get('status')
        if status == 'success':
            logger.info(f"  ✅ Created ticket {result.get('ticket_id')}")
            successful += 1
        elif status == 'already_exists':
            logger.info(f"  ⏭️  Already exists: {result.get('existing_tickets')}")
            successful += 1
        else:
            logger.error(f"  ❌ Failed: {result}")
            failed += 1

    logger.info(f"\n{'='*40}")
    logger.info(f"✅ Successful: {successful}")
    logger.info(f"❌ Failed:     {failed}")

if __name__ == '__main__':
    main()
