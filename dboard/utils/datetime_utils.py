from sqlalchemy import func
from datetime import datetime


def tz_str(mins):
    prefix = "+" if mins >= 0 else "-"
    return "%s%02d:%02d" % (prefix, abs(mins) / 60, abs(mins) % 60)


def tz_convert(datetime_col, timedelta_mins):
    return func.convert_tz(
        datetime_col, '+00:00', tz_str(timedelta_mins))


def tz_converted_date(datetime_col, timedelta_mins):
    return func.date(tz_convert(datetime_col, timedelta_mins))


def next_month_start(dt):
    month = (dt.month + 1) % 12
    if month == 0:
        month = 12
    if dt.month == 12:
        year = dt.year
    else:
        year = dt.year + int((dt.month + 1) / 12)
    return datetime(year, month, 1, 0, 0)


def this_month_start(dt):
    return datetime(dt.year, dt.month, 1, 0, 0)


def month_format(datetime_col):
    return func.date_format(datetime_col, "%Y-%m")

def date_format(datetime_col):
    return func.date_format(datetime_col, "%Y-%m-%d")

