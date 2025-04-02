"""
Utility functions for the project data pipeline
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Load credentials
load_dotenv()


def get_engine() -> Engine:
    """
    Create a connection to Azure SQL with credentials from
    environment variables.

    Environment variables used:
        AZURE_SQL_SERVER
        AZURE_SQL_DATABASE
        AZURE_SQL_USERNAME
        AZURE_SQL_PASSWORD
        AZURE_SQL_DRIVER (optional)

    Returns:
        Engine: The connection to the Azure SQL database
    """
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

    driver_encoded = driver.replace(" ", "+")
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}:1433/"
        f"{database}?driver={driver_encoded}"
    )

    # Connect to DB
    engine = create_engine(connection_string)

    return engine
