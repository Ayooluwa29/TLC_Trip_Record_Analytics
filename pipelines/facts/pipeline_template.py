import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
EDW_CONN = os.environ.get("EDW_CON")
EDW_ENGINE = create_engine(EDW_CONN)
budget_path = os.path.join(r"E:\Cloud_Files\OneDrive - AIICO INSURANCE\DMA Team\EDW_Budget", "Non_life_Product_Budget.xlsx")
TARGET_SCHEMA = "base"
table_name = "nonlife_product_budget"
table_name_with_schema = f"{TARGET_SCHEMA}.{table_name}"

dtype_mapping = {
    'object': 'TEXT',
    'int64': 'BIGINT',
    'float64': 'NUMERIC',
    'datetime64[ns]': 'DATE',
    'bool': 'BOOLEAN',
    'category': 'TEXT'
}

with EDW_ENGINE.begin() as conn:
    df_nonlife_product_budget = pd.read_excel(budget_path)

    columns_definition = []
    for col_name, dtype in df_nonlife_product_budget.dtypes.items():
        sql_dtype = dtype_mapping.get(str(dtype), 'TEXT')
        columns_definition.append(f'"{col_name}" {sql_dtype}')

    create_table_statement = text(f'CREATE TABLE IF NOT EXISTS {table_name_with_schema} ({", ".join(columns_definition)})')
    conn.execute(create_table_statement)
    print(f"Created table if not exists: {table_name_with_schema}")

    print(f"Truncating table: {table_name_with_schema}")
    conn.execute(text(f'TRUNCATE TABLE {table_name_with_schema}'))

    try:
        df_nonlife_product_budget.to_sql(table_name, conn, schema=TARGET_SCHEMA, if_exists="append", index=False)
        print(f"Loaded {len(df_nonlife_product_budget)} records into {table_name_with_schema}.")
    except Exception as e:
        print(f"Failed to load data into {table_name_with_schema}: {e}")