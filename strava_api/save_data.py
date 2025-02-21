# save_data.py
import polars as pl

def save_to_csv(df: pl.DataFrame, filename: str):
    """
    Saves a Polars DataFrame to a CSV file.
    Handles nested structures by flattening them to string representations.
    """
    try:
        # Create a new DataFrame with flattened columns
        processed_cols = []
        
        for col in df.columns:
            col_type = df[col].dtype
            
            if isinstance(col_type, pl.List):
                # Convert list columns to string using list comprehension and join
                processed_cols.append(
                    pl.col(col).map_elements(lambda x: ','.join(str(i) for i in x) if x is not None else '').alias(col)
                )
            elif isinstance(col_type, pl.Struct):
                # Convert struct columns to string representation
                processed_cols.append(pl.col(col).cast(str).alias(col))
            else:
                # Keep other columns as is
                processed_cols.append(pl.col(col))
        
        # Create new DataFrame with processed columns
        processed_df = df.select(processed_cols)
        
        # Write to CSV
        processed_df.write_csv(filename)
        print(f"Data saved successfully to {filename}")
        
    except Exception as e:
        print(f"An error occurred while saving to CSV: {e}")