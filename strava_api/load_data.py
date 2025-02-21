import logging
from typing import Optional, Union
import pandas as pd
import polars as pl
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

from strava_api.load_env import load_env_variables

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine using environment variables.
    
    Returns:
        SQLAlchemy Engine instance
    """
    env_vars = load_env_variables()
    
    # URL encode the password to handle special characters
    encoded_password = quote_plus(env_vars['password'])
    
    database_url = (
        f"postgresql+psycopg2://{env_vars['user']}:{encoded_password}"
        f"@{env_vars['host']}:{env_vars['port']}/{env_vars['dbname']}"
        "?sslmode=require"
    )
    
    logger.debug(f"Connecting to database at {env_vars['host']}")
    
    return create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30
    )

def validate_table_schema(engine: Engine, df: pd.DataFrame, table_name: str) -> bool:
    """
    Validate that DataFrame schema matches existing table schema.
    
    Args:
        engine: SQLAlchemy engine
        df: pandas DataFrame to validate
        table_name: name of the target table
        
    Returns:
        bool: True if schema matches or table doesn't exist
    """
    inspector = inspect(engine)
    
    if not inspector.has_table(table_name):
        logger.info(f"Table {table_name} does not exist - will be created")
        return True

    existing_columns = {
        col['name']: str(col['type']).upper() 
        for col in inspector.get_columns(table_name)
    }
    
    # Map pandas dtypes to PostgreSQL types
    type_mapping = {
        'float64': {'REAL', 'DOUBLE PRECISION', 'FLOAT'},
        'int64': {'INTEGER', 'BIGINT'},
        'bool': {'BOOLEAN'},
        'object': {'TEXT', 'VARCHAR', 'CHARACTER VARYING'},
        'datetime64[ns]': {'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP', 'DATE'},
        'datetime64[ms]': {'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP', 'DATE'},
    }
    
    mismatched_columns = []
    for col, dtype in df.dtypes.items():
        if col in existing_columns:
            dtype_name = dtype.name
            pg_type = existing_columns[col]
            
            # Check if the PostgreSQL type is compatible with the pandas dtype
            compatible_types = type_mapping.get(dtype_name, set())
            
            # Special handling for FLOAT(n) format
            if 'FLOAT' in pg_type or 'REAL' in pg_type:
                compatible_types.add('FLOAT')
                if dtype_name != 'float64' and not (dtype_name.startswith('float') or dtype_name.startswith('int')):
                    mismatched_columns.append(f"{col}: {dtype_name} vs {pg_type}")
            # Special handling for INTEGER
            elif 'INTEGER' in pg_type and dtype_name == 'float64':
                # Convert float columns to integer if they're going into INTEGER columns
                df[col] = df[col].round().astype('int64')
            elif pg_type not in compatible_types:
                mismatched_columns.append(f"{col}: {dtype_name} vs {pg_type}")
    
    if mismatched_columns:
        logger.error(f"Schema mismatch for columns: {', '.join(mismatched_columns)}")
        return False
        
    return True

def load_data(df: Union[pd.DataFrame, pl.DataFrame], table_name: str = "activities", primary_keys: list = None) -> bool:
    """
    Loads a DataFrame into PostgreSQL database using SQLAlchemy, skipping existing records.
    
    Args:
        df: data to load (pandas DataFrame or polars DataFrame)
        table_name: target table name
        primary_keys: list of column names that form the primary key (e.g., ['athlete_id', 'activity_id'])
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Set default primary keys for activities table
    if table_name == 'activities' and primary_keys is None:
        primary_keys = ['athlete_id', 'activity_id']

    # Convert to pandas DataFrame if necessary
    if isinstance(df, pl.DataFrame):
        try:
            df = df.to_pandas()
            # Cast specific columns to match PostgreSQL schema
            if table_name == 'activities':
                df['calories'] = df['calories'].round().astype('int64')
                # Convert date to datetime64[ns] and normalize to midnight
                df['date'] = pd.to_datetime(df['date']).dt.normalize()
                float_cols = ['distance', 'moving_time', 'elapsed_time', 'average_speed', 
                            'max_speed', 'average_cadence', 'average_heartrate', 
                            'max_heartrate', 'elev_high', 'elev_low']
                df[float_cols] = df[float_cols].astype('float64')
            logger.info("Successfully converted Polars DataFrame to pandas DataFrame with proper types")
        except Exception as e:
            logger.error(f"Failed to convert Polars DataFrame to pandas: {str(e)}")
            return False
    elif not isinstance(df, pd.DataFrame):
        try:
            df = pd.DataFrame(df)
            logger.info("Successfully converted input data to Pandas DataFrame")
        except Exception as e:
            logger.error(f"Failed to convert input to DataFrame: {str(e)}")
            return False

    engine = get_engine()
    try:
        # Validate schema before loading
        if not validate_table_schema(engine, df, table_name):
            raise ValueError("Schema validation failed")
            
        # If we have primary keys defined, check for existing records
        if primary_keys:
            with engine.connect() as conn:
                # Create a temporary table with just the primary keys
                temp_df = df[primary_keys].copy()
                temp_table = f"temp_{table_name}_check"
                
                # Load primary keys to temporary table
                temp_df.to_sql(temp_table, conn, if_exists='replace', index=False)
                
                # Query to find records that don't exist in the main table
                query = f"""
                    SELECT t.* FROM {temp_table} t
                    LEFT JOIN {table_name} m ON {' AND '.join(f't.{pk} = m.{pk}' for pk in primary_keys)}
                    WHERE {' OR '.join(f'm.{pk} IS NULL' for pk in primary_keys)}
                """
                
                # Get the new records' primary keys
                new_records = pd.read_sql(query, conn)
                
                # Drop temporary table
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                
                if len(new_records) == 0:
                    logger.info("No new records to insert to the database")
                    return True  # Exit cleanly with a success status
                
                # Filter the original DataFrame to only include new records
                df = df.merge(new_records, on=primary_keys, how='inner')
                
                logger.info(f"Found {len(df)} new records to insert")

        # Perform the data load
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        row_count = len(df)
        logger.info(f"Successfully loaded {row_count} rows into {table_name}")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Database error during load: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during load: {str(e)}")
        return False
    finally:
        engine.dispose()

def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Database connection test successful")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False
    finally:
        engine.dispose()

if __name__ == "__main__":
    # Test the connection when running this script directly
    try:
        if test_connection():
            # Example usage with both pandas and polars DataFrames
            test_pd_df = pd.DataFrame({
                'athlete_id': [1, 2, 3],
                'activity_id': [101, 102, 103],
                'value': [f'test_{i}' for i in range(3)]
            })
            test_pl_df = pl.DataFrame({
                'athlete_id': [1, 2, 3],
                'activity_id': [101, 102, 103],
                'value': [f'test_{i}' for i in range(3)]
            })
            
            # Test both types of DataFrames
            load_data(test_pd_df, 'activities')
            load_data(test_pl_df, 'activities')
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")