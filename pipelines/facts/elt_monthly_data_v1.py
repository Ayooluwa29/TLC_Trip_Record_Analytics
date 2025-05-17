import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
DW_CONN = os.environ.get("psql_conn")
DW_ENGINE = create_engine(DW_CONN)
files_folder = r"C:\Users\ajesuniyi\Documents\TLC_Trip"
TARGET_SCHEMA = "bronze"
table_name = "tlc_trip_yellow_taxi_2022"
table_name_with_schema = f"{TARGET_SCHEMA}.{table_name}"


dtype_mapping = {
    'object': 'TEXT',
    'int64': 'BIGINT',
    'float64': 'NUMERIC',
    'datetime64[ns]': 'DATE',
    'bool': 'BOOLEAN',
    'category': 'TEXT'
}

with DW_ENGINE.begin() as conn:
    first_file_processed = False
    for filename in os.listdir(files_folder):
        if filename.endswith('.parquet'):
            file_path = os.path.join(files_folder, filename)
            print(f"Processing file: {filename}")
            df = pd.read_parquet(file_path)

            if not first_file_processed:
                columns_definition = []
                for col_name, dtype in df.dtypes.items():
                    sql_dtype = dtype_mapping.get(str(dtype), 'TEXT')
                    columns_definition.append(f'"{col_name}" {sql_dtype}')

                create_table_statement = text(f'CREATE TABLE IF NOT EXISTS {table_name_with_schema} ({", ".join(columns_definition)})')
                conn.execute(create_table_statement)
                print(f"Created table if not exists: {table_name_with_schema}")

                print(f"Truncating table: {table_name_with_schema}")
                conn.execute(text(f'TRUNCATE TABLE {table_name_with_schema}'))
                first_file_processed = True

            try:
                df.to_sql(table_name, conn, schema=TARGET_SCHEMA, if_exists="append", index=False, chunksize= 10000)
                print(f"Loaded {len(df)} records from {filename} into {table_name_with_schema}.")
            except Exception as e:
                print(f"Failed to load data from {filename} into {table_name_with_schema}: {e}")