import os
from turtle import pd
import requests
import pandas as pd

def ingest_specific_medias(media_ids):
    # 1. Fetch credentials securely from environment variables
    api_token = os.getenv("WISTIA_API_TOKEN")
    if not api_token:
        raise ValueError("Please set the WISTIA_API_TOKEN environment variable.")

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    ingested_data = []

    print(f"Starting targeted ingestion for {len(media_ids)} media IDs...")

    for media_id in media_ids:
        # Construct the URL for the specific asset
        url = f"https://api.wistia.com/modern/stats/medias/{media_id}.json"
        # url = f"https://api.wistia.com/modern/stats/medias/{media_id}/engagement.json"
   
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            media_record = response.json()
            ingested_data.append(media_record)
            print(f" Successfully ingested: {media_id} (Title: '{media_record.get('name')}')")
        elif response.status_code == 404:
            print(f"❌ Error: Media ID {media_id} not found. Check if the ID is correct or deleted.")
        else:
            print(f"❌ Error fetching {media_id}: Status {response.status_code} - {response.text}")

    return ingested_data

if __name__ == "__main__":
    # The specific IDs you want to ingest
    target_ids = ["8hunphufxp", "9k4tbcdfg0"]
    
    try:
        results = ingest_specific_medias(target_ids)
        df_json = pd.json_normalize(results)
        df = pd.DataFrame(df_json)
        df["updated_at"] = pd.Timestamp.now()
        print(df.head(100))  # Display the first few rows of the ingested data
        print(f"\nIngested {len(results)} out of {len(target_ids)} records.")
        
        # At this point, 'results' contains the full JSON payload for both videos
        # including name, description, duration, and asset links (mp4/HLS URLs).
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")





# Starting targeted ingestion for 2 media IDs...
#  Successfully ingested: 8hunphufxp (Title: 'None')
#  Successfully ingested: 9k4tbcdfg0 (Title: 'None')
#    load_count  play_count  play_rate  hours_watched  engagement  visitors                 updated_at
# 0      115938         922   0.010566      37.022135    0.146208     85270 2026-09-16 16:26:13.528776
# 1         932         328   0.423680      51.176190    0.255894       701 2026-09-16 16:26:13.528776