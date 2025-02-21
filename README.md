# Strava Analytics Engineering

A data engineering project that automates the extraction, transformation, and loading (ETL) of Strava activity data into Supabase for analytics and visualization.

## Features

- **Automated Data Pipeline**: Fetches activities from Strava using their API.
- **Data Transformation**: Cleans and processes data using Polars.
- **Database Integration**: Stores processed data in Supabase (PostgreSQL backend).
- **FastAPI Webhook**: Enables real-time data updates when new activities are recorded.
- **Data Visualization**: Connects Supabase to Looker Studio for reporting.

## Tech Stack

- **Python**: Core scripting language
- **uv**: Project and package management
- **FastAPI**: Webhook for real-time updates
- **Polars**: High-performance data transformation
- **SQLAlchemy**: Database interaction
- **Supabase**: PostgreSQL database
- **Render**: Deployment platform
- **ngrok**: For testing the FastAPI Webhook API locally

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/strava-analytics-engineering.git
cd strava-analytics-engineering
```

### 2. Install Dependencies

Use uv for package mangement:

```bash
uv venv
source .venv/bin/activate  # On Windows, use '.venv\Scripts\activate'
uv pip install -r requirements.txt
```

### 3. Set Up Environment Variables

Create a `.env` file and add:

```
CLIENT_ID=your_strava_client_id
CLIENT_SECRET=your_strava_client_secret
REFRESH_TOKEN=your_strava_refresh_token
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_api_key
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=your_db_port
DB_NAME=your_db_name
```

### 4. Run the ETL Pipeline

```bash
python local_etl.py
```

### 5. Start the FastAPI Webhook

```bash
uvicorn webhook:app --host 0.0.0.0 --port 8000
```

## API Endpoints

- **`POST /webhook`**: Receives Strava activity updates.

## Test webhook locally using ngrok
```bash
ngrok http 8000
```

---

## License

This project is licensed under the MIT License.