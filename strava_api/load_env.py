import os
import logging
from typing import Dict
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_env_variables(env_file: str = ".env") -> Dict[str, str]:
    """
    Loads environment variables from a .env file.
    Returns a dictionary containing both Strava API and database connection variables.
    
    Args:
        env_file: Path to the environment file
        
    Returns:
        Dictionary with environment variables
        
    Raises:
        Exception: If any required variables are missing
    """
    if not load_dotenv(env_file, override=True):
        logger.warning(f"No {env_file} file found or error loading it")
    
    # Strava API variables
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    refresh_token = os.getenv("REFRESH_TOKEN")
    
    # Database connection variables
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    DBNAME = os.getenv("DBNAME")
    
    # Validate that all required variables are set
    missing = []
    if not client_id: missing.append("CLIENT_ID")
    if not client_secret: missing.append("CLIENT_SECRET")
    if not refresh_token: missing.append("REFRESH_TOKEN")
    if not USER: missing.append("USER")
    if not PASSWORD: missing.append("PASSWORD")
    if not HOST: missing.append("HOST")
    if not PORT: missing.append("PORT")
    if not DBNAME: missing.append("DBNAME")
    
    if missing:
        raise Exception(f"Missing environment variables: {', '.join(missing)}")
    
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "user": USER,
        "password": PASSWORD,
        "host": HOST,
        "port": PORT,
        "dbname": DBNAME,
    }

if __name__ == "__main__":
    try:
        env_vars = load_env_variables()
        logger.info("Environment variables loaded successfully")
    except Exception as e:
        logger.error(f"Error loading environment variables: {str(e)}")