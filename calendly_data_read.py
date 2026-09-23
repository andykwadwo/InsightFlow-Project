import io
import json
import boto3
import pandas as pd

# Initialize the standard S3 client
s3 = boto3.client("s3")

bucket_name = "dea-data-bucket"
folder_prefix = "calendly_spend_data"
target_bucket = "calandly-market-bucket-oseikwadwo"
target_prefix = "calendly_market_raw_data"

# Output configuration
output_key = f"{folder_prefix}/output/combined_spend_data.csv"  # or .parquet / .json

# 1. Fetch the index file using direct GetObject (doesn't trigger ListObjectsV2)
index_object = s3.get_object(
    Bucket=bucket_name, Key=f"{folder_prefix}/file_index.json"
)
index_json = json.loads(index_object["Body"].read().decode("utf-8"))

all_data = []

# 2. Iterate through the file names explicitly
for file_path in index_json["files"]:
  clean_path = file_path.lstrip("/")

  # Reconstruct the exact object key
  file_key = f"{folder_prefix}/{clean_path}"

  try:
    # Fetch the file directly by its key names
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    raw_data = response["Body"].read()

    # Load the raw bytes into pandas using a BytesIO buffer
    individual_df = pd.read_json(io.BytesIO(raw_data))
    all_data.append(individual_df)
    print(f"Successfully retrieved: {file_key}")

  except Exception as e:
    print(f"Error retrieving {file_key}: {e}")

# 3. Safely combine everything and write back to S3
if all_data:
  combined_spend_df = pd.concat(all_data, ignore_index=True)
  print("\nMaster Dataframe Head:")
  print(combined_spend_df.head())

  # 4. Serialize to in-memory buffer and upload
  csv_buffer = io.StringIO()
  combined_spend_df.to_csv(csv_buffer, index=False)

  try:
    s3.put_object(
        Bucket=target_bucket,
        Key=target_prefix + "/combined_spend_data.csv",
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"\n[SUCCESS] File uploaded to s3://{target_bucket}/{target_prefix}/combined_spend_data.csv")
  except Exception as e:
    print(
        f"\n[ERROR] Upload failed (ensure 's3:PutObject' permission exists):"
        f" {e}"
    )

else:
  print(
      "\n[ERROR] Direct file downloads failed. Your IAM credentials require"
      " 's3:GetObject' access."
  )
