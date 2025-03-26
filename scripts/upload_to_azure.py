import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# 1. Load CSV
csv_path = "./data/degustations.csv"
df = pd.read_csv(csv_path, sep=';', encoding='utf-8')

# 2. Clean the data
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

print(df.head())
print(df.columns)

# 3. Get Azure SQL credentials from environment variables
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("AZURE_SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")

driver_encoded = driver.replace(" ", "+")

# 4. Create connection string
connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver_encoded}"

# 5. Connect and upload
engine = create_engine(connection_string)
inspector = inspect(engine)
table_name = "degustations"
if not inspector.has_table(table_name):
    print("Table does not exist. Creating...")
    df.to_sql(table_name, con=engine, index=False)
    print(f"Data uploaded to Azure SQL table: {table_name}")
