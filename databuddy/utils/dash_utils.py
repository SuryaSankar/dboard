import dash_table
import numpy as np
import pandas as pd

from toolspy import merge
from flask import Response

from .formatters import format_value


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
            merge({
                k: format_value(v, field_types.get(k))
                for k, v in row.items()
            }, {
                "period": idx.strftime(
                    timestamp_col_name_format),
            }) for idx, row in zip(
                df.index, df.to_dict('records'))
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
        dt_rows = [merge({
            k.strftime(
                timestamp_col_name_format): format_value(
                v, field_types.get(idx))
            for k, v in row.items()}, {
                "metric": idx.replace("_", " ").capitalize()}
        ) for idx, row in zip(df.index, df.to_dict('records'))]
    return dash_table.DataTable(
        id=dt_id or 'table',
        style_data={'whiteSpace': 'normal'},
        css=[{
            'selector': '.dash-cell div.dash-cell-value',
            'rule': 'display: inline; white-space: inherit;\
             overflow: inherit; text-overflow: inherit;'
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


def convert_csv_text_to_csv_response(csvtext):
    return Response(csvtext, mimetype="text/csv")


def convert_df_to_csv_response(df):
    return convert_csv_text_to_csv_response(
        df.to_csv(encoding='utf-8'))


def convert_dt_to_csv_response(dt, index_col=None):
    return convert_df_to_csv_response(
        df=convert_dt_to_df(dt, index_col=index_col),
    )


def convert_dt_data_to_csv_response(dt_data, index_col=None):
    return convert_df_to_csv_response(
        df=convert_dt_data_to_df(dt_data, index_col=index_col)
    )
