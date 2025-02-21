import polars as pl
import pandas as pd
from datetime import datetime

def convert_pace(speed_m_per_s):
    """
    Converts speed (m/s) to pace in 'min:sec/km' format.
    """
    if speed_m_per_s is None or speed_m_per_s == 0:
        return "N/A"
    
    try:
        speed_km_per_min = float(speed_m_per_s) * 60 / 1000  # Convert m/s to km/min
        minutes_per_km = 1 / speed_km_per_min  # Convert speed to time per km

        minutes = int(minutes_per_km)
        seconds = round((minutes_per_km - minutes) * 60)

        if seconds == 60:
            minutes += 1
            seconds = 0

        return f"{minutes}:{seconds:02d}/km"
    except (ValueError, ZeroDivisionError):
        return "N/A"

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the Strava activity data with null handling and type conversions.

    Parameters:
        df (pd.DataFrame): Raw Strava activity data.
    
    Returns:
        pd.DataFrame: Transformed activity data.
    """
    # Convert pandas DataFrame to a Polars DataFrame
    pl_df = pl.from_pandas(df)

    # Extract athlete_id from the "athlete" struct field
    try:
        athlete_id_expr = pl.col("athlete").struct.field("id").alias("athlete_id")
    except Exception as e:
        print(f"Error extracting athlete_id: {e}")
        athlete_id_expr = pl.lit(None).alias("athlete_id")

    # Define calories expression using kilojoules if available
    if "kilojoules" in pl_df.columns:
        calories_expr = pl.col("kilojoules").fill_null(0).mul(0.239).round(0).alias("calories")
    else:
        calories_expr = pl.lit(0).alias("calories")

    try:
        transformed_df = (
            pl_df
            .with_columns([
                athlete_id_expr,
                (pl.col("distance").fill_null(0) / 1000).round(2).alias("distance"),
                (pl.col("moving_time").fill_null(0) / 60).round(2).alias("moving_time"),
                (pl.col("elapsed_time").fill_null(0) / 60).round(2).alias("elapsed_time"),
                # Updated date parsing to handle UTC timezone
                pl.col("start_date_local")
                  .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ", strict=False)
                  .cast(pl.Date)
                  .alias("date"),
                calories_expr,
                pl.col("average_speed").fill_null(0).round(2).alias("average_speed"),
                pl.col("max_speed").fill_null(0).round(2).alias("max_speed"),
                pl.col("average_cadence").fill_null(0).round(2).alias("average_cadence"),
                pl.col("elev_high").fill_null(0).round(2).alias("elev_high"),
                pl.col("elev_low").fill_null(0).round(2).alias("elev_low"),
                pl.col("has_heartrate").cast(pl.Boolean).fill_null(False).alias("has_heartrate"),
                pl.col("average_heartrate").fill_null(0).round(1).alias("average_heartrate"),
                pl.col("max_heartrate").fill_null(0).round(1).alias("max_heartrate"),
                pl.col("id").cast(pl.Int64).alias("activity_id"),
                pl.coalesce(pl.col("sport_type"), pl.col("type")).alias("sport")
            ])
            .with_columns([
                pl.col("average_speed").map_elements(convert_pace, return_dtype=pl.Utf8).alias("pace")
            ])
            .select([
                "athlete_id",
                "activity_id",
                "name",
                "distance",
                "moving_time",
                "elapsed_time",
                "sport",
                "date",
                "average_speed",
                "max_speed",
                "average_cadence",
                "calories",
                "has_heartrate",
                "average_heartrate",
                "max_heartrate",
                "elev_high",
                "elev_low",
                "pace"
            ])
        )
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise

    # Convert back to pandas DataFrame
    return transformed_df.to_pandas()
