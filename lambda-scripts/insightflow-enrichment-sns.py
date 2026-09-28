import ast
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

LOOKUP_BUCKET = os.environ["LOOKUP_BUCKET"]
TARGET_BUCKET = os.environ["TARGET_BUCKET"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def discover_real_sns_arn(configured_value: str) -> str:
    """
    Scans the AWS region to translate plain topic names or cross-region mismatches
    into a completely verified, active Topic ARN string.
    """
    clean_value = configured_value.strip().replace('"', '').replace("'", "")
    
    # Extract the base topic name if a full ARN pattern was passed
    target_name = clean_value.split(":")[-1] if ":" in clean_value else clean_value
    
    try:
        paginator = sns.get_paginator("list_topics")
        for page in paginator.paginate():
            for topic in page.get("Topics", []):
                topic_arn = topic.get("TopicArn", "")
                if topic_arn.endswith(f":{target_name}"):
                    logger.info("🎯 DISCOVERED VALID TOPIC ARN: %s", topic_arn)
                    return topic_arn
    except Exception as e:
        logger.warning("Could not programmatically list SNS topics via API: %s", e)
        
    return clean_value


def clean_and_force_parse(payload) -> dict:
    """Aggressively converts strings, json text, or wrappers into a native dict."""
    if isinstance(payload, dict):
        return payload
    if not isinstance(payload, str):
        return {}

    cleaned = payload.strip()
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    if cleaned.startswith("'") and cleaned.endswith("'"):
        cleaned = cleaned[1:-1]
    
    cleaned = cleaned.replace('\\\\', '\\').replace('\\"', '"').replace("\\'", "'")

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    try:
        return ast.literal_eval(cleaned)
    except Exception:
        pass

    if '"data"' in payload or "'data'" in payload:
        try:
            match = re.search(r'(\{.*\})', cleaned)
            if match:
                return json.loads(match.group(1))
        except Exception:
            pass

    return {}


def find_field_anywhere(data, target_key, depth=0):
    """Deep searches for a key across nested layers and auto-unwraps hidden string maps."""
    if depth > 12:
        return None
    if not isinstance(data, dict):
        return None
        
    if target_key in data and data[target_key] is not None and str(data[target_key]).strip() != "":
        return data[target_key]
        
    for key, value in data.items():
        if isinstance(value, str) and any(marker in value for marker in ['{', '}', '"', "'"]):
            unwrapped = clean_and_force_parse(value)
            if unwrapped and isinstance(unwrapped, dict):
                found = find_field_anywhere(unwrapped, target_key, depth + 1)
                if found is not None:
                    return found
                    
        if isinstance(value, dict):
            found = find_field_anywhere(value, target_key, depth + 1)
            if found is not None:
                return found
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    found = find_field_anywhere(item, target_key, depth + 1)
                    if found is not None:
                        return found
    return None


def read_json_from_s3(bucket: str, key: str) -> dict:
    """Read and parse a JSON object from S3."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.exception("Unable to read s3://%s/%s. AWS error: %s", bucket, key, error_code)
        raise RuntimeError(f"Could not retrieve data from s3://{bucket}/{key}") from exc
    except json.JSONDecodeError as exc:
        logger.exception("File at s3://%s/%s is not valid JSON.", bucket, key)
        raise RuntimeError("The S3 file contains invalid JSON.") from exc


def lambda_handler(event, context):
    logger.info("Received raw SQS event: %s", json.dumps(event))
    response = None
    processed_alerts = []

    # Resolve and verify the true SNS topic ARN configuration immediately upon handler wakeup
    verified_sns_arn = discover_real_sns_arn(SNS_TOPIC_ARN)

    for record in event.get("Records", []):
        try:
            # Step 1: Parse the SQS message body to extract the S3 Event Notification
            sqs_body = json.loads(record.get("body", "{}"))
            
            # Step 2: Extract bucket and key details safely
            s3_event_records = sqs_body.get("Records", [])
            if not s3_event_records or not isinstance(s3_event_records, list):
                logger.warning("No S3 event notification records found in SQS body. Skipping.")
                continue
                
            # Safely grab index 0 from the list array block
            s3_record = s3_event_records[0].get("s3", {})
            source_bucket = s3_record.get("bucket", {}).get("name")
            source_key = unquote_plus(s3_record.get("object", {}).get("key", ""))
            
            logger.info("Retrieving raw incoming webhook file from s3://%s/%s", source_bucket, source_key)
            
            # Step 3: Download the raw file containing the actual Postman/API Gateway payload
            raw_file_content = read_json_from_s3(source_bucket, source_key)
            
            normalized_map = clean_and_force_parse(raw_file_content)
            
        except Exception as exc:
            logger.error("Failed to parse S3 notification or download source file. Skipping. Error: %s", exc)
            continue

        # Step 4: Scan and pull values dynamically out of the nested structure
        try:
            display_name = find_field_anywhere(normalized_map, "display_name")
            if not display_name:
                raise ValueError("Missing required CRM event field: display_name")
                
            lead_id = (
                find_field_anywhere(normalized_map, "lead_id") or 
                find_field_anywhere(normalized_map, "object_id") or 
                find_field_anywhere(normalized_map, "id")
            )
            if not lead_id:
                raise ValueError("Missing required CRM event field: lead_id")
                
            date_created = find_field_anywhere(normalized_map, "date_created") or datetime.now(timezone.utc).isoformat()
            status_label = find_field_anywhere(normalized_map, "status_label") or "Unknown Status"
            
        except ValueError as exc:
            logger.error("Validation failed for CRM properties inside file content: %s", exc)
            continue

        logger.info("🚀 SUCCESS! Core Fields Found -> Name: %s, LeadID: %s", display_name, lead_id)

        # Step 5: Pull the complete supplemental lookup profile data from S3
        lookup_key = f"{lead_id}.json"
        try:
            lead_lookup = read_json_from_s3(LOOKUP_BUCKET, lookup_key)
        except Exception:
            logger.warning("Lookup query failed for lead_id=%s. Using default strings.", lead_id)
            lead_lookup = None

        if not lead_lookup:
            lead_email, lead_owner, funnel = "Not found", "Not found", "Not found"
        else:
            lead_email = lead_lookup.get("lead_email", "Not found")
            lead_owner = lead_lookup.get("lead_owner", "Not found")
            funnel = lead_lookup.get("funnel", "Not found")

        alert_data = {
            "display_name": display_name,
            "lead_id": lead_id,
            "date_created": date_created,
            "status_label": status_label,
            "lead_email": lead_email,
            "lead_owner": lead_owner,
            "funnel": funnel
        }

        # Step 6: Save the final compiled file into TARGET_BUCKET
        try:
            now = datetime.now(timezone.utc)
            target_key = f"enriched/{now:%Y/%m/%d}/{now:%H%M%S}-{lead_id}.json"
            
            s3.put_object(
                Bucket=TARGET_BUCKET,
                Key=target_key,
                Body=json.dumps(alert_data, default=str).encode("utf-8"),
                ContentType="application/json",
                ServerSideEncryption="AES256"
            )
            logger.info("Enriched file safely deployed to s3://%s/%s", TARGET_BUCKET, target_key)
        except Exception as exc:
            logger.error("Failed to compile target data onto S3 for lead_id=%s: %s", lead_id, exc)

        # Step 7: Build the alert message block and dispatch to SNS
        alert_message = (
            "New Lead Alert\n"
            f"Name: {display_name}\n"
            f"Lead ID: {lead_id}\n"
            f"Created Date: {date_created}\n"
            f"Label: {status_label}\n"
            f"Email: {lead_email}\n"
            f"Lead Owner: {lead_owner}\n"
            f"Funnel: {funnel}"
        )

        try:
            sns_kwargs = {
                "TopicArn": verified_sns_arn,
                "Subject": f"CRM Lead Alert: {display_name}",
                "Message": alert_message
            }
            
            if verified_sns_arn.endswith(".fifo"):
                sns_kwargs["MessageGroupId"] = "crm_lead_alerts"
                sns_kwargs["MessageDeduplicationId"] = f"{lead_id}-{int(datetime.now(timezone.utc).timestamp())}"

            response = sns.publish(**sns_kwargs)
            logger.info("Dispatched notification status updates to SNS for lead_id=%s", lead_id)
        except ClientError as exc:
            logger.error("Failed to push alert updates into SNS: %s", exc)
    return {"statusCode": 200, "body": json.dumps(response)}

