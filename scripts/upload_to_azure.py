import os
import re
import ast
import json
import unicodedata
import pandas as pd
from typing import Optional
from datetime import datetime
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Load environment variables from .env file
load_dotenv()

def clean_column_name(col: str) -> str:
    """
    Clean a column name by:
    - Replacing non-ASCII characters with closest equivalent
    - Converting to lowercase
    - Replacing non-alphanumeric characters with "_"
    - Removing leading and trailing "_"
    """
    col_clean = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
    col_clean = col_clean.lower()
    col_clean = re.sub(r"[^\w]+", "_", col_clean)
    col_clean = col_clean.strip("_")
    return col_clean

def create_column_name_mapping(df: pd.DataFrame) -> dict:
    """
    Create a mapping of the current column names to their cleaned versions.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame for which to generate the mapping.

    Returns a dictionary mapping from original column names to cleaned column names.
    """
    mapping = {col: clean_column_name(col) for col in df.columns}
    return mapping

def translate_schedule(schedule: str) -> Optional[dict]:
    """
    Translates a value from 'ouverturegranule' field into a dictionary.

    Schedule string has the format:
    "start_date||end_date||special_note_on_openning||special_note_on_closing||
     morning_open_0||morning_close_0||afternoon_open_0||afternoon_close_0||...||afternoon_close_6"

    Returns a dict with parsed date ranges and opening hours by weekday or None if the input is invalid.
    """
    current_year = datetime.now().year
    translated = []

    print("Translating schedule:", schedule)

    parts = schedule.split("||")
    if len(parts) < 4:
        return None

    translated_schedule = {
        "start_date": parts[0] or None,
        "end_date": parts[1] or None,
        "special_note_on_openning": parts[2] or None,
        "special_note_on_closing": parts[3] or None,
    }

    # Parse dates
    try:
        start_date = datetime.strptime(parts[0], "%d/%m/%Y")
        end_date = datetime.strptime(parts[1], "%d/%m/%Y")
        # Add a note if the current year is not included in the date range
        if start_date.year != current_year and end_date.year != current_year:
            translated_schedule["year_note"] = "Current year is not included in the date range"
    except ValueError:
        translated_schedule["year_note"] = "No year in schedule"

    weekly_hours = parts[4:]
    formatted_hours = {}

    for i in range(7):  # 7 days
        base_idx = i * 4
        formatted_hours[DAYS_OF_WEEK[i]] = {
            "morning_opening": weekly_hours[base_idx] if base_idx < len(weekly_hours) else "",
            "morning_closing": weekly_hours[base_idx + 1] if base_idx + 1 < len(weekly_hours) else "",
            "afternoon_opening": weekly_hours[base_idx + 2] if base_idx + 2 < len(weekly_hours) else "",
            "afternoon_closing": weekly_hours[base_idx + 3] if base_idx + 3 < len(weekly_hours) else ""
        }

    translated_schedule["schedule_by_day"] = formatted_hours

    return translated_schedule

def translate_schedules(schedules: Optional[list]) -> Optional[list[dict]]:
    """
    Translates an 'ouverturegranule' field into a human-readable textual description.

    This function takes a list of strings, each representing a schedule.
    A schedule is a string with the following format:
    "start_date||end_date||special_note_on_openning||special_note_on_closing||morning_0||afternoon_0||morning_1||afternoon_1||...||morning_6||afternoon_6"

    """
    if not schedules:
        return None
    
    translated_schedules = []
    for schedule in schedules:
        translated_schedules.append(translate_schedule(schedule))

    return translated_schedules

def process_schedules_column(df):
    # Translate and convert to JSON string per row
    def clean_schedule(schedules):
        if pd.notnull(schedules):
            try:
                translated = translate_schedules(schedules)
                return json.dumps(translated, ensure_ascii=False) if translated else None
            except Exception as e:
                print(f"Error translating schedule: {schedules}\n{e}")
                return None
        else:
            return None

    df["horaires_traduits"] = df["horaires_d_ouvertures"].apply(clean_schedule)
    return df


# Load CSV
csv_path = "./data/degustations.csv"
df = pd.read_csv(csv_path, sep=';', encoding='utf-8')


# Create mapping and save it to JSON file
col_mapping = create_column_name_mapping(df)
with open("column_mapping.json", "w", encoding="utf-8") as f:
    json.dump(col_mapping, f, ensure_ascii=False, indent=2)

# Apply cleaned names to dataframe
df.rename(columns=col_mapping, inplace=True)

df["horaires_d_ouvertures"] = df["horaires_d_ouvertures"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x] if x else None
)


print(df[df["horaires_d_ouvertures"].notnull()][["horaires_d_ouvertures"]])

df = process_schedules_column(df)

print(df[df["horaires_traduits"].notnull()][["horaires_traduits"]])

# print(df.head())
# print(df.columns)

# Get Azure SQL credentials from environment variables
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("AZURE_SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")

driver_encoded = driver.replace(" ", "+")

# Create connection string
connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver_encoded}"

# Connect and upload
engine = create_engine(connection_string)
inspector = inspect(engine)
table_name = "degustations"
if not inspector.has_table(table_name):
    print("Table does not exist. Creating...")
    df.to_sql(table_name, con=engine, index=False)
    print(f"Data uploaded to Azure SQL table: {table_name}")
