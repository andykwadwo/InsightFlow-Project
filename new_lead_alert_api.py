import requests
import os
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
headers = {"Content-type": "application/json"}
data = {"text": "Hello, world."}

response = requests.post(SLACK_WEBHOOK_URL, json=data, headers=headers)
print(response.status_code) # Should return 200 if successful