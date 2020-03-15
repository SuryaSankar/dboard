"""Main module."""
from sqlalchemy.orm import scoped_session, sessionmaker

import pandas as pd
import numpy as np
import math
from toolspy import merge, subdict
from toolspy.datetime_tools import n_days_ago
from sqlalchemy import func
import urllib.parse

import json
from flask import render_template_string, request
from flask_sqlalchemy_booster.responses import as_json
from datetime import datetime, timedelta
from sqlalchemy import asc, desc
import dash_table

import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

QUERY_MODIFIERS = [
    'page', 'per_page', 'limit', 'offset', 'order_by', 'sort', 'group_by']


def sqla_sort(sort_order):
    return asc if sort_order == 'asc' else desc

def fetch_query_modifiers_from_request():
    return subdict(
        request.args,
        QUERY_MODIFIERS
    )


def construct_query_modifiers(
        query_modifiers=None, allow_modification_via_requests=True):
    default_query_modifiers = {
        "page": None,
        "per_page": 20,
        "limit": None,
        "offset": None,
        "order_by": None,
        "sort": "asc",
        "group_by": None
    }
    print("received query modifiers as ", query_modifiers)
    if query_modifiers is None:
        query_modifiers = {}
    query_modifiers = subdict(query_modifiers, QUERY_MODIFIERS)
    query_modifiers = merge(
        default_query_modifiers, query_modifiers)
    if allow_modification_via_requests:
        query_modifiers = merge(
            query_modifiers, fetch_query_modifiers_from_request())
    print("final query_modifiers ", query_modifiers)
    return query_modifiers


def apply_modifiers_on_sqla_query(
        q, page=None, per_page=20, limit=None, offset=None,
        group_by=None, order_by=None, sort='asc'):
    if group_by is not None:
        q = q.group_by(group_by)
    if order_by is not None:
        q = q.order_by(sqla_sort(sort)(order_by))
    if page:
        per_page = int(per_page)
        q = q.limit(per_page).offset((int(page) - 1) * per_page)
    elif limit and offset:
        q = q.limit(limit).offset(int(offset) - 1)
    return q


def get_queried_field_labels(q):
    return [e._label_name for e in q._entities]

def construct_meta_dict_from_query(q, query_modifiers):
    meta = {
        "total_items": q.count(),
    }
    if query_modifiers.get("page"):
        meta["page"] = query_modifiers["page"]
        meta["per_page"] = query_modifiers.get("per_page")
        meta["total_pages"] = math.ceil(
            meta["total_items"] / meta["per_page"])
        meta["columns"] = get_queried_field_labels(q)
    return meta


def construct_json_response_from_query(
        q, query_modifiers=None, allow_modification_via_requests=True):

    query_modifiers = construct_query_modifiers(
        query_modifiers,
        allow_modification_via_requests=allow_modification_via_requests)
    meta = construct_meta_dict_from_query(q, query_modifiers)

    q = apply_modifiers_on_sqla_query(q, **query_modifiers)

    result = q.all()
    # q.session.remove()

    return as_json(
        convert_sqla_collection_items_to_dicts(
            result
        ),
        meta=meta,
        struct_key="data"
    )


def convert_sqla_collection_item_to_dict(item):
    return {f: getattr(item, f) for f in item._fields}


def convert_sqla_collection_items_to_dicts(collection):
    return [convert_sqla_collection_item_to_dict(item) for item in collection]


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


def convert_query_to_df(query, index_col):
    return pd.read_sql(
        query, engine, index_col=index_col)


def format_as_currency(v):
    return "Rs. {:,.2f}".format(v)


def format_as_percentage(v):
    return "{:.2f} %".format(v)


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


class SqlaQueryBuilder(object):

    def __init__(self, engine, flask_app=None, timedelta_mins_from_utc=0):
        self.engine = engine
        self.timedelta_mins_from_utc = timedelta_mins_from_utc
        self.session = scoped_session(sessionmaker(bind=self.engine))

        if flask_app:
            @flask_app.teardown_appcontext
            def shutdown_session(response_or_exc):
                print("about to remove session")
                self.session.remove()
                return response_or_exc

    def local_time(self):
        return datetime.utcnow() + timedelta(
            minutes=self.timedelta_mins_from_utc)

    def local_tz_converted_date(self, datetime_col):
        return tz_converted_date(
            datetime_col, self.timedelta_mins_from_utc)

    def local_n_days_ago(self, n):
        return n_days_ago(
            n, timedelta_mins_from_utc=self.timedelta_mins_from_utc)

    def construct_query(
            self, fields_to_query, joins=None, filters=None):
        q = self.session.query(*fields_to_query)
        if filters:
            q = q.filter(*filters)
        return q

    def construct_interval_query(
            self, interval_field_name, 
            interval_field_label, interval_timestamp_format,
            fields_to_query=None, filters=None):
        interval_field = func.date_format(
            self.local_tz_converted_date(interval_field_name), interval_timestamp_format)
        fields_to_query = [interval_field.label(interval_field_label)] + (fields_to_query or [])
        q = self.session.query(*fields_to_query)
        if filters:
            q = q.filter(*filters)
        q = q.group_by(interval_field)
        # q = q.order_by(interval_field)
        return q

    def convert_interval_query_to_df(
            self, query, interval_field_label, 
            interval_timestamp_format):
        return pd.read_sql(
            query, self.engine, parse_dates={interval_field_label: interval_timestamp_format}, 
            index_col=interval_field_label)

    def construct_interval_df(
            self, interval_field_name,
            interval_field_label, interval_timestamp_format,
            fields_to_query=None, filters=None):
        return self.convert_interval_query_to_df(
            self.construct_interval_query(
                interval_field_name,
                interval_field_label,
                interval_timestamp_format,
                fields_to_query=fields_to_query, filters=filters
            ).subquery(),
            interval_field_label,
            interval_timestamp_format
        )

    def construct_daily_query(
            self, day_field_name, fields_to_query=None,
            filters=None, day_field_label='day'):
        return self.construct_interval_query(
            interval_field_name=day_field_name, 
            interval_field_label=day_field_label, 
            interval_timestamp_format='%Y-%m-%d',
            fields_to_query=fields_to_query, filters=filters
        )

    def convert_daily_query_to_df(
            self, query, day_field_label='day'):
        return self.convert_interval_query_to_df(
            query, interval_field_label=day_field_label,
            interval_timestamp_format='%Y-%m-%d')

    def construct_daily_df(
            self, day_field_name, fields_to_query=None, 
            filters=None, day_field_label='day'):
        return self.construct_interval_df(
            interval_field_name=day_field_name, 
            interval_field_label=day_field_label, 
            interval_timestamp_format='%Y-%m-%d',
            fields_to_query=fields_to_query, filters=filters
        )

    def construct_monthly_query(
            self, month_field_name, fields_to_query=None, 
            filters=None, month_field_label='month'):
        return self.construct_interval_query(
            interval_field_name=month_field_name,
            interval_field_label=month_field_label,
            interval_timestamp_format='%Y-%m',
            fields_to_query=fields_to_query, filters=filters
        )

    def convert_monthly_query_to_df(query, month_field_label='month'):
        return pd.read_sql(query, self.engine, parse_dates={month_field_label: '%Y-%m'}, index_col=month_field_label)


    def construct_monthly_df(
            self, month_field_name, fields_to_query=None,
            filters=None, month_field_label='month'):
        return self.convert_monthly_query_to_df(
            self.construct_monthly_query(
                month_field_name,
                fields_to_query=fields_to_query, filters=filters, 
                month_field_label=month_field_label
            ).subquery(),
            month_field_label=month_field_label
        )
