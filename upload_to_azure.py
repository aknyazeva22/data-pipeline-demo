import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# 1. Load CSV
csv_path = "../data/degustations.csv"
df = pd.read_csv(csv_path)

# 2. Clean the data
df.dropna(inplace=True)  # Drop rows with missing values
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# 3. Get Azure SQL credentials from environment variables
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# 4. Create connection string
connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver.replace(' ', '+')}"

# 5. Connect and upload
engine = create_engine(connection_string)
table_name = "degustations"  # Change this as needed

df.to_sql(table_name, con=engine, if_exists="replace", index=False)

print(f"Data uploaded to Azure SQL table: {table_name}")
