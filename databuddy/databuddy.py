"""Main module."""
from sqlalchemy.orm import Session

import pandas as pd
import numpy as np
from toolspy import merge
from toolspy.datetime_tools import n_days_ago
from sqlalchemy import func
import urllib.parse

import json
from flask import render_template_string, request
from datetime import datetime, timedelta

import dash_table

import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool


def tz_str(mins):
    prefix = "+" if mins >= 0 else "-"
    return "%s%02d:%02d" % (prefix, abs(mins) / 60, abs(mins) % 60)


def tz_convert(datetime_col, timedelta_mins):
    GMT_TZ_STR = '+00:00'
    return func.convert_tz(datetime_col, GMT_TZ_STR, tz_str(timedelta_mins))


def tz_converted_date(datetime_col, timedelta_mins):
    return func.date(tz_convert(datetime_col, timedelta_mins))


def groupby_result_to_pd_series(result):
    return pd.Series({k: v for k, v in result})


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


def construct_query(fields_to_query, joins=None, filters=None, session=None):
    q = session.query(*fields_to_query)
    if filters:
        q = q.filter(*filters)
    return q


def construct_interval_query(
        interval_field_name, interval_field_label, interval_timestamp_format,
        fields_to_query=None, filters=None, session=None):
    interval_field = func.date_format(
        local_tz_converted_date(interval_field_name), interval_timestamp_format)
    fields_to_query = [interval_field.label(interval_field_label)] + (fields_to_query or [])
    q = session.query(*fields_to_query)
    if filters:
        q = q.filter(*filters)
    q = q.group_by(interval_field).order_by(interval_field)
    return q


def convert_query_to_df(query, index_col):
    return pd.read_sql(
        query, engine, index_col=index_col)


def convert_interval_query_to_df(query, interval_field_label, interval_timestamp_format):
    return pd.read_sql(
        query, engine, parse_dates={interval_field_label: interval_timestamp_format}, 
        index_col=interval_field_label)


def construct_interval_df(
        interval_field_name, interval_field_label, interval_timestamp_format,
        fields_to_query=None, filters=None):
    return convert_interval_query_to_df(
        construct_interval_query(
            interval_field_name,
            interval_field_label,
            interval_timestamp_format,
            fields_to_query=fields_to_query, filters=filters
        ).subquery(),
        interval_field_label,
        interval_timestamp_format
    )

def construct_daily_query(
        day_field_name, fields_to_query=None, filters=None, day_field_label='day'):
    return construct_interval_query(
        interval_field_name=day_field_name, 
        interval_field_label=day_field_label, 
        interval_timestamp_format='%Y-%m-%d',
        fields_to_query=fields_to_query, filters=filters
    )

def convert_daily_query_to_df(query, day_field_label='day'):
    return convert_interval_query_to_df(
        query, interval_field_label=day_field_label,
        interval_timestamp_format='%Y-%m-%d')


def construct_daily_df(
        day_field_name, fields_to_query=None, filters=None, day_field_label='day'):
    return construct_interval_df(
        interval_field_name=day_field_name, 
        interval_field_label=day_field_label, 
        interval_timestamp_format='%Y-%m-%d',
        fields_to_query=fields_to_query, filters=filters
    )


def construct_monthly_query(
        month_field_name, fields_to_query=None, 
        filters=None, month_field_label='month',
        session=None):
    month_field = func.date_format(local_tz_converted_date(month_field_name), '%Y-%m')
    fields_to_query = [month_field.label(month_field_label)] + (fields_to_query or [])
    q = session.query(*fields_to_query)
    if filters:
        q = q.filter(*filters)
    q = q.group_by(month_field).order_by(month_field)
    return q


def convert_monthly_query_to_df(query, engine, month_field_label='month'):
    return pd.read_sql(query, engine, parse_dates={month_field_label: '%Y-%m'}, index_col=month_field_label)


def construct_monthly_df(month_field_name, fields_to_query=None, filters=None, month_field_label='month'):
    return convert_monthly_query_to_df(
        construct_monthly_query(
            month_field_name,
            fields_to_query=fields_to_query, filters=filters, 
            month_field_label=month_field_label
        ).subquery(),
        month_field_label=month_field_label
    )


def format_as_currency(v):
    return "Rs. {:,.2f}".format(v)

def format_as_percentage(v):
    return  "{:.2f} %".format(v)

def format_value(value, field_type=None):
    if field_type == 'currency':
        return format_as_currency(value)
    if field_type == 'percentage':
        return format_as_percentage(value)
    return value

def null_safe_sum(col):
    return func.sum(func.ifnull(col, 0))

def convert_field_type_fields_list_mapping_to_field_field_type_mapping(
        field_type_fields_list_mapping):
    return {
        field: field_type
        for field_type, fields in field_type_fields_list_mapping.items() 
        for field in fields
    } if types_of_fields else {}

def convert_timestamp_indexed_df_to_dt(
        df, dt_id=None, types_of_fields=None,
        timestamp_col_name_format="%b %Y"):
    print("in timestamp indexed df to dt")
    print(df)
    df = df.replace(np.nan, 0, regex=True)
    field_types = {
        field: field_type
        for field_type, fields in types_of_fields.items() 
        for field in fields
    } if types_of_fields else {}
    if isinstance(df.index, pd.DatetimeIndex):
        dt_columns = [
            {"name": "Period", "id": "period"}] + [
            {
                "name": col_name.replace("_", " ").capitalize(), 
                "id": col_name
            } for col_name in df.columns
        ]
        dt_rows = [
                {
                    "period": idx.strftime(timestamp_col_name_format), 
                    **{
                        k: format_value(v, field_types.get(k)) 
                        for k, v in row.items()
                    }
                } for idx, row in zip(df.index, df.to_dict('records'))
        ]
    else:
        print("in else block")
        dt_columns = [
            {"name": "Metric", "id": "metric"}] + [
            {
                "name": i.strftime(timestamp_col_name_format), 
                "id": i.strftime(timestamp_col_name_format)
            } for i in df.columns
        ]
        dt_rows = [
                {
                    "metric": idx.replace("_", " ").capitalize(),
                    **{k.strftime(timestamp_col_name_format): format_value(v, field_types.get(idx)) for k, v in row.items()}
                } for idx, row in zip(df.index, df.to_dict('records'))
        ]
    return dash_table.DataTable(
        id=dt_id or 'table',
        style_data={'whiteSpace': 'normal'},
        css=[{
            'selector': '.dash-cell div.dash-cell-value',
            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
        }],
        columns=dt_columns,
        data=dt_rows,
    )

def convert_daily_df_to_dt(
        df, dt_id=None, types_of_fields=None):
    return convert_timestamp_indexed_df_to_dt(
        df, dt_id=dt_id, 
        types_of_fields=types_of_fields,
        timestamp_col_name_format="%d %b %Y"
    )

def convert_monthly_df_to_dt(
        df, dt_id=None, types_of_fields=None):
    return convert_timestamp_indexed_df_to_dt(
        df, dt_id=dt_id,
        types_of_fields=types_of_fields,
        timestamp_col_name_format="%b %Y"
    )


def convert_dt_to_df(dt, index_col=None):
    df = pd.DataFrame(dt.data).replace(np.nan, 0, regex=True)
    if index_col:
        df = df.set_index(index_col)
    return df

def convert_dt_data_to_df(dt_data, index_col=None):
    df = pd.DataFrame(dt_data).replace(np.nan, 0, regex=True)
    if index_col:
        df = df.set_index(index_col)
    return df

def convert_df_to_csv_response(df):
    return "data:text/csv;charset=utf-8,{}".format(
        urllib.parse.quote(df.to_csv(encoding='utf-8'))
    )

def convert_dt_to_csv_response(dt, index_col=None):
    return convert_df_to_csv_response(
        df=convert_dt_to_df(dt, index_col=index_col),
    )


def convert_dt_data_to_csv_response(dt_data, index_col=None):
    return convert_df_to_csv_response(
        df=convert_dt_data_to_df(dt_data, index_col=index_col)
    )

def run_function_and_store_return_value(
        function=None, args=None, kwargs=None, return_queue=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    return_value = function(*args, **kwargs)
    if return_queue:
        return_queue.put((function.__name__, return_value))


def wrap_function_in_process_with_return_queue(
        function=None, args=None, kwargs=None, return_queue=None):
    return multiprocessing.Process(
        target=run_function_and_store_return_value,
        kwargs={
            "function": function,
            "args": args,
            "kwargs": kwargs,
            "return_queue": return_queue
        })

def function_runner(function=None, args=None, kwargs=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    return function(*args, **kwargs)


def run_in_threads(func_args_kwargs_list):
    pool = ThreadPool()
    return pool.starmap(function_runner, func_args_kwargs_list)


def run_in_processes(func_args_kwargs_list):
    pool = multiprocessing.Pool()
    return pool.starmap(function_runner, func_args_kwargs_list)

