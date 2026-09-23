import requests
import os
from requests.auth import HTTPBasicAuth
from datetime import datetime

# Configuration
API_TOKEN = os.getenv("WISTIA_API_TOKEN")
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]
BASE_URL = "https://api.wistia.com/modern"


# Setup authentication (Wistia uses the token as the password with a blank username)
auth = HTTPBasicAuth("api", API_TOKEN)

def fetch_wistia_media(created_after=None):
    """
    Extracts media metadata and core engagement metrics.
    Implements pagination and incremental ingestion via 'created_after'.
    """
    endpoint = f"{BASE_URL}/medias.json"
    page = 1
    per_page = 100
    all_media = []

    while True:
        params = {
            "page": page,
            "per_page": per_page
        }
        
        # Incremental ingestion filter
        if created_after:
            params["created_after"] = created_after

        response = requests.get(endpoint, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        for item in data:
            # Filter for your specific target Media IDs
            if item.get("hashed_id") in MEDIA_IDS:
                # Extracting requested metadata and high-level engagement stats
                media_payload = {
                    "id": item.get("id"),
                    "hashed_id": item.get("hashed_id"),
                    "title": item.get("name"),
                    "created_at": item.get("created"),
                    "status": item.get("status"),
                    # Core engagement metrics packaged in the media endpoint
                    "plays": item.get("stats", {}).get("plays"),
                    "play_rate": item.get("stats", {}).get("playRate"),
                    "watch_time": item.get("stats", {}).get("watchTime"),
                }
                all_media.append(media_payload)

        if len(data) < per_page:
            break
        page += 1

    return all_media

def fetch_visitor_engagement(media_id):
    """
    Extracts granular visitor-level data and engagement events for a specific media asset.
    Uses Wistia's Stats API / Account Media Stats.
    """
    # Note: Granular visitor events typically leverage Wistia's Data API or Stats event endpoints
    # endpoint = f"{BASE_URL}/stats/medias/{media_id}/engagement.json"
    # endpoint = f"{BASE_URL}/stats/medias/{media_id}/by_visitor.json"
    endpoint = f"{BASE_URL}/stats/medias/{media_id}.json"
    
    try:
        response = requests.get(endpoint, auth=auth)
        response.raise_for_status()
        return response.json()  # Contains IP, visitor keys, and playback events
    except requests.exceptions.HTTPError as e:
        print(f"Could not fetch visitor stats for {media_id}: {e}")
        return None

# Execution Example
if __name__ == "__main__":
    # Example starting timestamp for incremental load (UTC format)
    last_ingestion_time = "2026-01-01T00:00:00Z" 
    
    print("Extracting media metadata and core metrics...")
    media_data = fetch_wistia_media(created_after=last_ingestion_time)
    
    print(f"Found {len(media_data)} matching media assets.")
    
    print("\nExtracting granular visitor-level engagement data...")
    for media in media_data:
        hashed_id = media["hashed_id"]
        print(f"Fetching visitor stream for Media: {hashed_id}")
        visitor_stats = fetch_visitor_engagement(hashed_id)
        
        # Here you would route media_payload and visitor_stats to your database/data warehouse
        print(f"Media Metadata: {media}")
        print(f"Visitor Stats Sample: {str(visitor_stats)[:200]}...\n")
