"""
Send SQL queries from file `./sql/basic_queries.sql`
to Azure SQL
"""

from sqlalchemy import text
from scripts.utils import get_engine

# Load and execute SQL file
SQL_FILE_PATH = "./sql/basic_queries.sql"
with open(SQL_FILE_PATH, "r", encoding="utf-8") as file:
    queries = file.read()

engine = get_engine()

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
