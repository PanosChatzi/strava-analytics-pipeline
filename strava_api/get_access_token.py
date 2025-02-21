# get_access_token.py
import requests

def get_access_token(client_id: str, client_secret: str, refresh_token: str):
    """
    Uses the refresh token to get a new access token from Strava API.
    """
    token_url = f"https://www.strava.com/oauth/token?client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token"
    
    payload = {}
    headers = {}
    
    try:
        print("Requesting Token...\n")
        response = requests.post(token_url, headers=headers, data=payload)
        response.raise_for_status()  # This will raise an error if the response status code is not 200
        response_data = response.json()
        
        access_token = response_data.get('access_token')
        print("Access token received")
        return access_token
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # For example, 401, 404, etc.
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")  # Catch other possible exceptions (e.g., connection issues)
        raise
