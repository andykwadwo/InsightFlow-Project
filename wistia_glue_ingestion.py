import json
import boto3
import urllib3

def get_secret():
    # Fetch Wistia API token from AWS Secrets Manager
    secret_name = "wistia_api_token"
    region_name = "us-east-1"
    
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])['token']

def fetch_wistia_data(api_token):
    # Call Wistia Data API (e.g., fetching a list of projects or videos)
    http = urllib3.PoolManager()
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    url = "https://wistia.com"
    
    response = http.request('GET', url, headers=headers)
    if response.status == 200:
        return json.loads(response.data.decode('utf-8'))
    else:
        raise Exception(f"Failed to fetch data: {response.status}")

def upload_to_s3(data):
    s3 = boto3.client('s3')
    bucket_name = "your-target-s3-bucket"
    file_key = "wistia_ingestion/projects.json"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(data)
    )

# Main execution
token = get_secret()
wistia_data = fetch_wistia_data(token)
upload_to_s3(wistia_data)
