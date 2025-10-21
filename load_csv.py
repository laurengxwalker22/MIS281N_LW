import pandas as pd
from sqlalchemy import create_engine
import os

# --- Azure SQL Connection ---
server = os.environ['AZURE_SERVER']      # e.g., yourserver.database.windows.net
database = os.environ['AZURE_SQL_DATABASE']  # e.g., ConsumerEdgeDB
username = os.environ['AZURE_USER']
password = os.environ['AZURE_PASSWORD']

connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_url)

# --- Load CSVs ---
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
