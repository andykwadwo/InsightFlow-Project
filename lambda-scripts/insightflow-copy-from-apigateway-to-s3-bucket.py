import ast
import base64
import json
import os
from datetime import datetime, timezone
from urllib.parse import parse_qs # Added for form-urlencoded parsing
from uuid import uuid4

import boto3

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["EVENT_BUCKET_NAME"].strip()
PREFIX = os.environ.get("EVENT_PREFIX", "events").strip()

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    event_type = event.get("event-type", "unknown_type")
    version = event.get("version", "1.0")
    raw_payload_str = event.get("payload")

    # Proceed with safely converting the literal dictionary text block
    if raw_payload_str:
        try:
            event = ast.literal_eval(raw_payload_str)
        except (ValueError, SyntaxError) as e:
            print(f"Failed to cleanly evaluate internal dictionary string: {e}")

    object_key = (
        f"{PREFIX}/"
        f"{now:%Y/%m/%d}/"
        f"{now:%H%M%S}-{uuid4()}.json"
    )

    # 1. Check if the payload arrived via x-www-form-urlencoded
    raw_payload_str = None
    
    if isinstance(event, dict):
        body = event.get("body", "")
        
        # If API Gateway base64 encoded the form payload, decode it first
        if body and event.get("isBase64Encoded", False):
            try:
                body = base64.b64decode(body).decode("utf-8")
            except Exception:
                pass
        
        # Parse the form-urlencoded string (e.g., "payload=value")
        if body and "=" in body:
            parsed_form = parse_qs(body)
            # Extract our 'payload' key
            if "payload" in parsed_form:
                raw_payload_str = parsed_form["payload"][0]

    # 2. Safely evaluate the extracted Python string literal
    if raw_payload_str:
        try:
            event = ast.literal_eval(raw_payload_str)
        except (ValueError, SyntaxError) as e:
            print(f"Failed to evaluate literal from form data: {e}")
    elif isinstance(event, str):
        try:
            event = ast.literal_eval(event)
        except (ValueError, SyntaxError):
            pass

    # Process base64 inner body if it exists natively in your payload structure
    inner_body = event.get("body") if isinstance(event, dict) else None
    if inner_body and event.get("isBase64Encoded", False):
        try:
            decoded_inner = base64.b64decode(inner_body).decode("utf-8")
        except UnicodeDecodeError:
            decoded_inner = base64.b64decode(inner_body).hex()

        if isinstance(event, dict):
            event["body"] = decoded_inner
            event["isBase64Encoded"] = False

    # Package record data securely
    record = {
        "receivedAt": now.isoformat(),
        "requestId": context.aws_request_id,
        "functionName": context.function_name,
        "apiGatewayEvent": event,
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(record, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    return {
        "statusCode": 201,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message": "Event saved to S3",
            "bucket": BUCKET_NAME,
            "key": object_key
        })
    }
