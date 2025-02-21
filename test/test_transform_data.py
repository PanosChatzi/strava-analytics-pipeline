import pytest
import polars as pl
from strava_api.transform_data import convert_pace, transform_data

# Test convert_pace function
@pytest.mark.parametrize("speed, expected_pace", [
    (3.33, "5:00/km"),  # 3.33 m/s ≈ 5:00/km
    (2.78, "6:00/km"),  # 2.78 m/s ≈ 6:00/km
    (5.00, "3:20/km"),  # 5.00 m/s ≈ 3:20/km
    (0, "N/A"),         # Edge case: speed is zero
])
def test_convert_pace(speed, expected_pace):
    assert convert_pace(speed) == expected_pace

# Test transform_data function
def test_transform_data():
    # Create a mock DataFrame
    df = pl.DataFrame({
        "athlete": [{"id": 12345}],  # Struct with athlete_id
        "id": [1001],
        "name": ["Morning Run"],
        "distance": [5000],  # 5 km
        "moving_time": [1500],  # 25 min
        "elapsed_time": [1600],  # 26.67 min
        "type": ["Run"],
        "start_date_local": ["2023-05-15T08:30:00Z"],
        "average_speed": [3.33],  # 5:00/km pace
        "max_speed": [5.5],
        "average_cadence": [85.567],
        "kilojoules": [500],
        "has_heartrate": [True],
        "average_heartrate": [145.678],
        "max_heartrate": [180.234],
        "elev_high": [100.567],
        "elev_low": [50.234],
    })

    # Transform the data
    df_transformed = transform_data(df)

    # Assert correct transformations
    assert df_transformed["athlete_id"][0] == 12345
    assert df_transformed["workout_id"][0] == 1001
    assert df_transformed["name"][0] == "Morning Run"
    assert df_transformed["distance"][0] == 5.00  # Converted to km, rounded to 2 decimals
    assert df_transformed["moving_time"][0] == 25.00  # Converted to min
    assert df_transformed["elapsed_time"][0] == 26.67  # Rounded to 2 decimals
    assert df_transformed["sport"][0] == "Run"
    assert df_transformed["date"][0].strftime("%Y-%m-%d") == "2023-05-15"
    assert df_transformed["average_speed"][0] == 3.33
    assert df_transformed["max_speed"][0] == 5.50
    assert df_transformed["average_cadence"][0] == 85.57  # Rounded to 2 decimals
    assert df_transformed["calories"][0] == 120  # 500 kJ * 0.239, rounded to 0
    assert df_transformed["has_heartrate"][0] == True
    assert df_transformed["average_heartrate"][0] == 145.7  # Rounded to 1 decimal
    assert df_transformed["max_heartrate"][0] == 180.2  # Rounded to 1 decimal
    assert df_transformed["elev_high"][0] == 100.57
    assert df_transformed["elev_low"][0] == 50.23
    assert df_transformed["pace"][0] == "5:00/km"  # Correct pace formatting