import pandas as pd
import os
from sqlalchemy import create_engine, engine

#Configure Postgres Destination
files_folder = r"C:\Users\ajesuniyi\Documents\TLC_Trip"


for filename in os.listdir(files_folder):
    if filename.endswith('.parquet'):
        file_path = os.path.join(files_folder, filename)
        print(f"Loading {file_path}...")

        # Read Parquet file
        df = pd.read_parquet(file_path)

        # Write to Postgres
        df.to_sql(name="tlc_trip_yellow_taxi_2022", con=psql_conn, if_exists='replace', index=False)

        print(f"Inserted {len(df)} rows from {filename}")