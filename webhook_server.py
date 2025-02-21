from fastapi import FastAPI, Request, HTTPException, Query
import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from strava_api.get_access_token import get_access_token
from strava_api.fetch_activities import fetch_single_activity
from strava_api.transform_data import transform_data
from typing import Dict, Any, Optional
from datetime import datetime

# Load environment variables
load_dotenv()

# Strava API credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# FastAPI app
app = FastAPI()

def prepare_for_supabase(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert timestamps and other non-serializable types to appropriate format."""
    processed_data = {}
    for key, value in data.items():
        if isinstance(value, pd.Timestamp):
            processed_data[key] = value.isoformat()
        elif isinstance(value, datetime):
            processed_data[key] = value.isoformat()
        else:
            processed_data[key] = value
    return processed_data

@app.get("/")
async def root() -> Dict[str, str]:
    return {"message": "Welcome to the FitnessDashboard webhook server!"}

@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {"status": "healthy"}

@app.get("/webhook")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
    hub_verify_token: str = Query(None, alias="hub.verify_token")
) -> Dict[str, str]:
    if not all([hub_mode, hub_challenge, hub_verify_token]):
        raise HTTPException(status_code=400, detail="Missing required parameters")
    
    if hub_verify_token == "my_verification_token":
        return {"hub.challenge": hub_challenge}
    
    raise HTTPException(status_code=403, detail="Invalid verify token")

@app.post("/webhook")
async def handle_webhook(request: Request) -> Dict[str, str]:
    try:
        event = await request.json()
        print("Webhook Data:", event)

        if event.get("object_type") == "activity":
            athlete_id = event.get("owner_id")
            activity_id = event.get("object_id")
            aspect_type = event.get("aspect_type")

            if aspect_type == "create":
                print(f"New activity detected: {activity_id}")
                await process_new_activity(athlete_id, activity_id)
            elif aspect_type == "update":
                print(f"Activity {activity_id} updated.")
                await process_updated_activity(athlete_id, activity_id)
            elif aspect_type == "delete":
                print(f"Activity {activity_id} deleted.")
                await delete_activity(activity_id)

        return {"status": "ok"}

    except Exception as e:
        print(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

async def process_new_activity(athlete_id: int, activity_id: int) -> None:
    try:
        print(f"Fetching activity details for activity_id: {activity_id}")
        
        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        
        activity_data = fetch_single_activity(access_token, activity_id)
        if not activity_data:
            print(f"Failed to fetch activity {activity_id}")
            return

        df = pd.DataFrame([activity_data])
        df_transformed = transform_data(df)

        # Check if activity exists
        result = supabase.table('activities').select("activity_id").eq('activity_id', activity_id).execute()
        
        if len(result.data) > 0:
            print(f"Activity {activity_id} already exists. Skipping insertion.")
            return

        # Convert DataFrame to dict and prepare for Supabase
        activity_dict = df_transformed.to_dict('records')[0]
        processed_dict = prepare_for_supabase(activity_dict)

        # Insert new activity
        result = supabase.table('activities').insert(processed_dict).execute()
        
        if not result.data:
            print(f"Warning: No confirmation data received for activity {activity_id}")
            return
            
        print(f"Stored activity {activity_id} in database.")

    except Exception as e:
        print(f"Error processing activity {activity_id}: {e}")
        raise  # Re-raise the exception to see the full traceback

async def process_updated_activity(athlete_id: int, activity_id: int) -> None:
    try:
        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        
        activity_data = fetch_single_activity(access_token, activity_id)
        if not activity_data:
            print(f"Failed to fetch updated activity {activity_id}")
            return

        df = pd.DataFrame([activity_data])
        df_transformed = transform_data(df)
        
        # Convert DataFrame to dict and prepare for Supabase
        activity_dict = df_transformed.to_dict('records')[0]
        processed_dict = prepare_for_supabase(activity_dict)

        # Update existing record
        result = supabase.table('activities').update(processed_dict).eq('activity_id', activity_id).execute()
        
        if not result.data:
            print(f"Warning: No confirmation data received for updating activity {activity_id}")
            return
            
        print(f"Updated activity {activity_id} in database.")

    except Exception as e:
        print(f"Error updating activity {activity_id}: {e}")
        raise

async def delete_activity(activity_id: int) -> None:
    try:
        result = supabase.table('activities').delete().eq('activity_id', activity_id).execute()
        
        if not result.data:
            print(f"Warning: No confirmation data received for deleting activity {activity_id}")
            return
            
        print(f"Deleted activity {activity_id} from database.")
    except Exception as e:
        print(f"Error deleting activity {activity_id}: {e}")
        raise  # Re-raise the exception to see the full traceback

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)