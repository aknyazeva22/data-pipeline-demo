import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load credentials
load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

driver_encoded = driver.replace(" ", "+")
connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver_encoded}"

# Connect to DB
engine = create_engine(connection_string)

# Load and execute SQL file
sql_file_path = "./sql/basic_queries.sql"
with open(sql_file_path, "r") as file:
    queries = file.read()

# Split queries on semicolon if needed
for query in queries.strip().split("\n"):
    print(query)
    query = query.strip()
    if query:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            print(f"Executed: {query}")
            for row in result:
                print(row)
