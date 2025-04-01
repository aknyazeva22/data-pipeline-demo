"""
Upload data from a CSV file to Azure SQL
"""
import ast
import json
import re
import unicodedata
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import inspect
from scripts.utils import get_engine

DAYS_OF_WEEK = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
PATH_TO_DEGUSTATIONS_FILE = "./data/degustations.csv"
TABLE_NAME = "degustations"

def clean_column_name(col: str) -> str:
    """
    Clean a column name by:
    - Replacing non-ASCII characters with closest equivalent
    - Converting to lowercase
    - Replacing non-alphanumeric characters with "_"
    - Removing leading and trailing "_"
    """
    col_clean = (
        unicodedata.normalize("NFKD", col)
        .encode("ASCII", "ignore")
        .decode("utf-8")
    )

    col_clean = col_clean.lower()
    col_clean = re.sub(r"[^\w]+", "_", col_clean)
    col_clean = col_clean.strip("_")
    return col_clean


def create_column_name_mapping(given_df: pd.DataFrame) -> dict:
    """
    Create a mapping of the current column names to their cleaned versions.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame for which to generate the mapping.

    Returns a dictionary mapping from original column names to
    cleaned column names.
    """
    mapping = {col: clean_column_name(col) for col in given_df.columns}
    return mapping


def translate_schedule(schedule: str) -> Optional[dict]:
    """
    Translates a value from 'horaires_d_ouvertures' field into a dictionary.

    Schedule string has the format:
    "start_date||end_date||special_note_on_openning||special_note_on_closing
    ||morning_open_0||morning_close_0||afternoon_open_0||afternoon_close_0||
    ...||afternoon_close_6"

    Returns a dict with parsed date ranges and opening hours by weekday
    or None if the input is invalid.
    """
    current_year = datetime.now().year

    parts = schedule.split("||")
    if len(parts) < 4:
        return None

    translated_schedule = {
        "start_date": parts[0] or None,
        "end_date": parts[1] or None,
        "special_note_on_openning": parts[2] or None,
        "special_note_on_closing": parts[3] or None,
    }

    translated_schedule["has_special_note"] = bool(
        translated_schedule["special_note_on_openning"]
        or translated_schedule["special_note_on_closing"]
    )

    # Parse dates
    try:
        start_date = datetime.strptime(parts[0], "%d/%m/%Y")
        end_date = datetime.strptime(parts[1], "%d/%m/%Y")
        # Check if the current year is included in the date range
        current_year_included = bool(
            start_date.year <= current_year <= end_date.year
        )
        translated_schedule["current_year_included"] = current_year_included
    except ValueError:
        translated_schedule["current_year_included"] = False

    weekly_hours = parts[4:]
    formatted_hours = {}

    for i in range(7):  # 7 days
        base_idx = i * 4
        formatted_hours[DAYS_OF_WEEK[i]] = {
            "morning_opening": (
                weekly_hours[base_idx] if base_idx < len(weekly_hours) else ""
            ),
            "morning_closing": (
                weekly_hours[base_idx + 1]
                if base_idx + 1 < len(weekly_hours)
                else ""
            ),
            "afternoon_opening": (
                weekly_hours[base_idx + 2]
                if base_idx + 2 < len(weekly_hours)
                else ""
            ),
            "afternoon_closing": (
                weekly_hours[base_idx + 3]
                if base_idx + 3 < len(weekly_hours)
                else ""
            ),
        }

    all_days_empty = all(
        all(t.strip() == "" for t in times.values())
        for times in formatted_hours.values()
    )
    translated_schedule["has_schedule_by_day"] = not all_days_empty

    translated_schedule["schedule_by_day"] = formatted_hours

    return translated_schedule


def select_preferable_schedules(schedules: list) -> list:
    """
    Select schedules with current year included and schedule by day.

    If there are such schedules, return them. Otherwise, return all schedules.
    Parameters:
    schedules (list): A list of translated schedules to select from,
        each represented as a dictionary.
    """
    chosen_schedules = []
    # choose schedules with current year included and schedule by day
    for schedule in schedules:
        if (
            schedule["current_year_included"]
            and schedule["has_schedule_by_day"]
        ):
            chosen_schedules.append(schedule)

    if chosen_schedules:
        return chosen_schedules

    return schedules


def translate_schedules(schedules: Optional[list]) -> Optional[list[dict]]:
    """
    Translate a list of schedules from string format to a list of dictionaries.

    Schedules are translated from string format to a dictionary format.

    If the input list is empty, return None.

    If there are several schedules, try to choose schedules with current year
    included and schedule by day, if possible. Otherwise, return all schedules.
    """
    if not schedules:
        return None

    translated_schedules = [
        translate_schedule(schedule) for schedule in schedules
    ]
    # select schedules with current year included and schedule by day
    preferable_schedules = select_preferable_schedules(translated_schedules)

    return preferable_schedules


def process_schedules_column(given_df):
    """
    Translate and convert to JSON string per row"
    """
    def clean_schedule(schedules):
        if pd.notnull(schedules):
            try:
                translated = translate_schedules(schedules)

                return (
                    json.dumps(translated, ensure_ascii=False)
                    if translated
                    else None
                )
            except ValueError as e:
                print(f"Error translating schedule: {schedules}\n{e}")
                return None
        else:
            return None

    given_df["horaires_traduits"] = given_df["horaires_d_ouvertures"].apply(clean_schedule)
    return given_df


def main():
    """
    Upload data from a CSV file to Azure SQL
    """
    # Load CSV
    df = pd.read_csv(PATH_TO_DEGUSTATIONS_FILE, sep=";", encoding="utf-8")

    # Create mapping and save it to JSON file
    col_mapping = create_column_name_mapping(df)
    with open("column_mapping.json", "w", encoding="utf-8") as f:
        json.dump(col_mapping, f, ensure_ascii=False, indent=2)

    # Apply cleaned names to dataframe
    df.rename(columns=col_mapping, inplace=True)

    df["horaires_d_ouvertures"] = df["horaires_d_ouvertures"].apply(
        lambda x: (
            ast.literal_eval(x)
            if isinstance(x, str) and x.startswith("[")
            else [x] if x else None
        )
    )
    # print(df[df["horaires_d_ouvertures"].notnull()][["horaires_d_ouvertures"]])
    df = process_schedules_column(df)

    # print(df[df["horaires_traduits"].notnull()][["horaires_traduits"]])
    # print(df.head())
    # print(df.columns)

    # Upload to Azure SQL
    engine = get_engine()
    inspector = inspect(engine)

    if not inspector.has_table(TABLE_NAME):
        print("Table does not exist. Creating...")
        df.to_sql(TABLE_NAME, con=engine, index=False)
        print(f"Data uploaded to Azure SQL table: {TABLE_NAME}")


if __name__ == "__main__":
    main()
