# Import necessary modules
import pandas as pd
import os
# dabases libraries
import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"/home/sysadmin/oracle_client/instantclient_19_26")
import oracledb
from sqlalchemy import create_engine, text
import pyodbc
# mail libraries
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#Connections
#Configure Oracle Source
TurnQuest = create_engine(f"oracle+oracledb://aiicobiuser:fKgp9FNaJt@tqdb-ho02.aiicoplc.com:4039/tqho02")
#Configure Postgres Destination
EDW_CONN = 'postgresql+psycopg2://aiicobiuser:fKgp9FNaJt@srv-bi02:5432/AIICO_EDW'
EDW_ENGINE = create_engine(EDW_CONN)

# List of postgres tables and their corresponding DataFrame names
tables_df_mapping = {
     #Grouplife
    "BI_GRP_PROD": "df_grp_production",
    "BI_GRP_COMMISSIONS": "df_grp_commissions",
    "BI_GRP_REPORTED_CLAIMS": "df_grp_reported_claims",
    "BI_GRP_PAID_CLAIMS": "df_grp_paid_claims",
    "BI_GRP_OUTSTANDING_CLAIMS": "df_grp_outstanding_claims",
    "BI_GRP_REINSURANCE": "df_grp_reinsurance"
}

# Mapping of DataFrames to destination table names
dataframe_to_table_mapping = {
    #Grouplife
    "df_grp_production": "fact_grouplife_production",
    "df_grp_commissions": "fact_grouplife_commissions",
    "df_grp_reported_claims": "fact_grouplife_reported_claims",
    "df_grp_paid_claims": "fact_grouplife_paid_claims",
    "df_grp_outstanding_claims": "fact_grouplife_outstanding_claims",
    "df_grp_reinsurance": "fact_grouplife_reinsurance"
}


# Schema in the PostgreSQL database
target_schema = "raw"

# extract data from source
def extract_data():
    """Fetch data from Oracle and store it in DataFrames."""
    global dataframes
    dataframes = {}

    for table, df_name in tables_df_mapping.items():
        query = f"SELECT * FROM {table}"
        print(f"Fetching data from {table}...")
        dataframes[df_name] = pd.read_sql(query, con=TurnQuest)
        print(f"Fetched {len(dataframes[df_name])} records into {df_name}.")


# truncate destination tables
def truncate_tables():
    """Truncate destination tables in PostgreSQL before loading new data."""
    with EDW_ENGINE.begin() as connection:
        for df_name, table_name in dataframe_to_table_mapping.items():
            full_table_name = f"{target_schema}.{table_name}"
            print(f"Truncating table '{full_table_name}'...")
            connection.execute(text(f"TRUNCATE TABLE {full_table_name}"))
            print(f"Table '{full_table_name}' truncated successfully.")


# load data into destination table
def load_data():
    """Load extracted data into PostgreSQL."""
    with EDW_ENGINE.begin() as connection:
        for df_name, table_name in dataframe_to_table_mapping.items():
            full_table_name = f"{target_schema}.{table_name}"

            if df_name in dataframes:
                df = dataframes[df_name]
                print(f"Loading data into '{full_table_name}'...")
                df.to_sql(
                    table_name,
                    EDW_ENGINE,
                    if_exists='append',
                    index=False,
                    schema=target_schema
                )
                print(f"Data loaded successfully into '{full_table_name}'.")
            else:
                print(f"DataFrame '{df_name}' not found. Skipping.")


# ETL status mail
def send_success_email():
    """Send email notification after successful ETL run."""
    smtp_server = 'smtp.mailgun.org'
    smtp_port = 587
    smtp_user = 'postmaster@aiicoplc.com'
    smtp_password = 'a0cbf1cf12982028c62f4b9a950ba4bf'

    from_email = 'biuser@aiicoplc.com'
    to_email = 'analytics-admin@aiicoplc.com'
    subject = 'EDW Group Life Facts ETL Status Notification'
    body = """
    Dear Analytics Team,

    The Enterprise Data Warehouse group-life fact tables data load completed successfully and tables were updated.

    Warm Regards,
    """

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to SMTP and send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
            print("✅ Success email sent!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")