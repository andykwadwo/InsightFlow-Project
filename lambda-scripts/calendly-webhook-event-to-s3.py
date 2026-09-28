import json
import os
from datetime import datetime
import boto3

# Initialize the S3 client outside the handler to optimize container reuse
s3_client = boto3.client("s3")

# Set these configuration variables directly or via Lambda Environment Variables
BUCKET_NAME = os.environ.get("BUCKET_NAME", "calandly-market-bucket-oseikwadwo")
FOLDER_PREFIX = os.environ.get("FOLDER_PREFIX", "calendly_webhook_data_raw")


def lambda_handler(event, context):
  try:
    # 1. Parse incoming webhook payload (Handles API Gateway / Function URL event payloads)
    if "body" in event:
      # Payload coming from an API Gateway proxy integration or Function URL
      payload_data = (
          json.loads(event["body"])
          if isinstance(event["body"], str)
          else event["body"]
      )
    else:
      # Direct payload invocation or alternative triggers
      payload_data = event

    # 2. Structure partitioning using execution or payload creation timestamp
    # Uses UTC execution time fallback if parsing created_at fails
    try:
      created_at_str = payload_data.get(
          "created_at", datetime.utcnow().isoformat()
      )
      dt = datetime.strptime(created_at_str[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
      dt = datetime.utcnow()

    # Create Hive-style partition formatting for clean downstream parsing
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    # 3. Generate a distinct file name using the timestamp and a unique request ID
    request_id = context.aws_request_id if context else "test-run"
    file_name = f"calendly_{dt.strftime('%H%M%S')}_{request_id}.json"

    # Construct complete target folder path mapping
    s3_key = f"{FOLDER_PREFIX}/year={year}/month={month}/day={day}/{file_name}"

    # 4. Upload raw JSON directly into S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(payload_data, indent=2),
        ContentType="application/json",
    )

    print(f"Successfully wrote webhook event to: s3://{BUCKET_NAME}/{s3_key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Webhook payload recorded successfully",
            "s3_path": f"s3://{BUCKET_NAME}/{s3_key}",
        }),
    }

  except Exception as e:
    print(f"Error handling webhook event invocation: {str(e)}")
    return {
        "statusCode": 500,
        "body": json.dumps({"error": "Internal server processing failure"}),
    }

