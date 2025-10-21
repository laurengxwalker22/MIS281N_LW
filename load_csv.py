import os
import pandas as pd
from sqlalchemy import create_engine

# --- Azure SQL Connection using environment variables ---
server = os.environ['AZURE_SQL_SERVER']
database = os.environ['AZURE_SQL_DATABASE']
username = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']

connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_url)

# --- Load CSVs into SQL tables ---
csv_files = {
    "brand_detail.csv": "BrandDetails",
    "daily_spend.csv": "DailySpend"
}

for file, table_name in csv_files.items():
    print(f"Loading {file} into table {table_name}...")
    df = pd.read_csv(file)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"{table_name} updated successfully!")

print("All CSVs loaded into Azure SQL Database!")
