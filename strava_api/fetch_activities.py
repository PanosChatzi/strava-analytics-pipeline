# fetch_activities.py
import requests

def fetch_activities(access_token: str, per_page: int = 200, page: int = 1):
    """
    Fetches activities from Strava using the provided access token.
    """
    activities_url = f"https://www.strava.com/api/v3/athlete/activities?access_token={access_token}"
    
    payload = {}
    headers = {}
    params = {'per_page': per_page, 'page': page}
    
    try:
        response = requests.get(activities_url, headers=headers, data=payload, params=params)
        response.raise_for_status()  # This will raise an error if the response status code is not 200
        activities = response.json()
        
        if not activities:
            print("No activities found.")
            raise ValueError("No activities were returned by the API.")

        return activities
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except ValueError as val_err:
        print(f"Value error occurred: {val_err}")
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise

import requests

# Fetch single activity for a given activity ID
def fetch_single_activity(access_token: str, activity_id: int):
    """
    Fetches a single activity from Strava using the activity ID.

    Parameters:
        access_token (str): The Strava API access token.
        activity_id (int): The ID of the activity to fetch.

    Returns:
        dict: The activity details if successful, else None.
    """
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()  # Return the activity data as a dictionary
    else:
        print(f"Error fetching activity {activity_id}: {response.text}")
        return None

