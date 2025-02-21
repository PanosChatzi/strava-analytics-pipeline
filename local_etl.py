from strava_api.load_env import load_env_variables
from strava_api.get_access_token import get_access_token
from strava_api.fetch_activities import fetch_activities
from strava_api.transform_data import transform_data
from strava_api.load_data import load_data

import polars as pl

def main():
    try:
        # Step 1: Load environment variables (as a dictionary)
        env_vars = load_env_variables()
        client_id = env_vars["client_id"]
        client_secret = env_vars["client_secret"]
        refresh_token = env_vars["refresh_token"]

        # Step 2: Get the access token
        access_token = get_access_token(client_id, client_secret, refresh_token)

        # Step 3: Fetch activities
        activities = fetch_activities(access_token)

        # Step 4: Convert activities to Polars DataFrame
        df = pl.DataFrame(activities)

        # Step 5: Clean and transform data
        df_transformed = transform_data(df)
        # Uncomment for debugging:
        # print(df_transformed.head())
        # print(f"DataFrame Shape: {df_transformed.shape}")
        
        # Step 6: Load data to Postgres database in Supabase
        load_data(df_transformed)  # This function performs the insertion

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
