import os
import pandas as pd
from sqlalchemy import create_engine

# --- Azure SQL Connection using environment variables ---
server = os.environ['AZURE_SQL_SERVER']
database = os.environ['AZURE_SQL_DATABASE']
username = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
engine = create_engine(conn_str)

# --- Load CSVs into SQL tables ---
csv_files = {
    "brand-detail-url-etc_0_0_0.csv": "BrandDetails",
    "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv": "DailySpend"
}

for file, table_name in csv_files.items():
    print(f"Loading {file} into table {table_name}...")
    df = pd.read_csv(file)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"{table_name} updated successfully!")

print("All CSVs loaded into Azure SQL Database!")
