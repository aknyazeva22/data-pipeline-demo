"""
Test work with schedules
"""
import pytest
from scripts.upload_to_azure import translate_schedule


SCHEDULE_1 = (
    "2022-01-01||2022-12-31||Special note||Closing note||09:00||12:00||14:00"
    "||18:00||09:00||12:00||14:00||18:00||09:00||12:00||14:00||18:00||09:00||12:00||"
    "14:00||18:00||09:00||12:00||14:00||18:00"
)
OUTPUT_1 = {
    'current_year_included': False,
    'end_date': '2022-12-31',
    'has_schedule_by_day': True,
    'has_special_note': True,
    'schedule_by_day': {
        'Friday': {
            'afternoon_closing': '18:00',
            'afternoon_opening': '14:00',
            'morning_closing': '12:00',
            'morning_opening': '09:00',
        },
        'Monday': {
            'afternoon_closing': '18:00',
            'afternoon_opening': '14:00',
            'morning_closing': '12:00',
            'morning_opening': '09:00',
        },
        'Saturday': {
            'afternoon_closing': '',
            'afternoon_opening': '',
            'morning_closing': '',
            'morning_opening': '',
        },
        'Sunday': {
            'afternoon_closing': '',
            'afternoon_opening': '',
            'morning_closing': '',
            'morning_opening': '',
        },
        'Thursday': {
            'afternoon_closing': '18:00',
            'afternoon_opening': '14:00',
            'morning_closing': '12:00',
            'morning_opening': '09:00',
        },
        'Tuesday': {
            'afternoon_closing': '18:00',
            'afternoon_opening': '14:00',
            'morning_closing': '12:00',
            'morning_opening': '09:00',
        },
        'Wednesday': {
            'afternoon_closing': '18:00',
            'afternoon_opening': '14:00',
            'morning_closing': '12:00',
            'morning_opening': '09:00',
        },
    },
    'special_note_on_closing': 'Closing note',
    'special_note_on_openning': 'Special note',
    'start_date': '2022-01-01',
}



@pytest.mark.parametrize("schedule, expected_result", [
    (SCHEDULE_1, OUTPUT_1),
    ("Invalid schedule format", None)
])
def test_translate_schedule(schedule, expected_result):
    """
    Test translation of schedules from string format to a dictionary.

    The test checks that correct schedule string is translated to correct dictionary
    and that invalid schedule string is translated to None.

    The test is parametrized with two examples: one valid schedule string and one
    invalid schedule string.
    """
    result = translate_schedule(schedule)
    assert result == expected_result
