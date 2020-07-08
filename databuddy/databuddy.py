"""Main module."""
from contextlib import contextmanager

from datetime import datetime, timedelta

from flask_sqlalchemy_session import flask_scoped_session

import pandas as pd

from sqlalchemy import func

from sqlalchemy.orm import scoped_session, sessionmaker

from toolspy.datetime_tools import n_days_ago

from .utils.datetime_utils import tz_converted_date, tz_convert


class SqlaQueryBuilder(object):

    def __init__(self, engine, flask_app=None, timedelta_mins_from_utc=0):
        self.engine = engine
        self.timedelta_mins_from_utc = timedelta_mins_from_utc
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.app = flask_app

        if flask_app:
            self.session = flask_scoped_session(self.sessionmaker, flask_app)
        else:
            self.session = scoped_session(self.sessionmaker)

    @contextmanager
    def scoped_session(self, commit=False):
        """Provide a transactional scope around a series of operations."""
        session = self.session()
        try:
            yield session
            if commit:
                session.commit()
        except:
            if commit:
                session.rollback()
            raise
        finally:
            session.close()

    def local_time(self):
        return datetime.utcnow() + timedelta(
            minutes=self.timedelta_mins_from_utc)

    def local_tz_converted_date(self, datetime_col):
        return tz_converted_date(
            datetime_col, self.timedelta_mins_from_utc)

    def local_tz_convert(self, datetime_col):
        return tz_convert(
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
            session=None,
            fields_to_query=None, filters=None):
        interval_field = func.date_format(
            self.local_tz_converted_date(
                interval_field_name),
            interval_timestamp_format)
        fields_to_query = [
            interval_field.label(interval_field_label)
        ] + (fields_to_query or [])
        if not session:
            session = self.session
        q = self.session.query(*fields_to_query)
        if filters:
            q = q.filter(*filters)
        q = q.group_by(interval_field)
        # q = q.order_by(interval_field)
        return q

    def convert_query_to_df(self, query, index_col):
        return pd.read_sql(
            query, self.engine, index_col=index_col)

    def convert_interval_query_to_df(
            self, query, interval_field_label,
            interval_timestamp_format):
        return pd.read_sql(
            query, self.engine,
            parse_dates={
                interval_field_label: interval_timestamp_format},
            index_col=interval_field_label)

    def construct_interval_df(
            self, interval_field_name,
            interval_field_label, interval_timestamp_format,
            session=None,
            fields_to_query=None, filters=None):
        return self.convert_interval_query_to_df(
            self.construct_interval_query(
                interval_field_name,
                interval_field_label,
                interval_timestamp_format,
                session=session,
                fields_to_query=fields_to_query, filters=filters
            ).subquery(),
            interval_field_label,
            interval_timestamp_format
        )

    def construct_daily_query(
            self, day_field_name, session=None,
            fields_to_query=None,
            filters=None, day_field_label='day'):
        return self.construct_interval_query(
            interval_field_name=day_field_name,
            interval_field_label=day_field_label,
            interval_timestamp_format='%Y-%m-%d',
            session=session,
            fields_to_query=fields_to_query, filters=filters
        )

    def convert_daily_query_to_df(
            self, query, day_field_label='day'):
        return self.convert_interval_query_to_df(
            query, interval_field_label=day_field_label,
            interval_timestamp_format='%Y-%m-%d')

    def construct_daily_df(
            self, day_field_name, fields_to_query=None,
            session=None,
            filters=None, day_field_label='day'):
        return self.construct_interval_df(
            interval_field_name=day_field_name,
            interval_field_label=day_field_label,
            interval_timestamp_format='%Y-%m-%d',
            session=session,
            fields_to_query=fields_to_query, filters=filters
        )

    def construct_monthly_query(
            self, month_field_name, fields_to_query=None,
            session=None, filters=None, month_field_label='month'):
        return self.construct_interval_query(
            interval_field_name=month_field_name,
            interval_field_label=month_field_label,
            interval_timestamp_format='%Y-%m',
            session=session,
            fields_to_query=fields_to_query, filters=filters
        )

    def convert_monthly_query_to_df(self, query, month_field_label='month'):
        return pd.read_sql(
            query, self.engine,
            parse_dates={month_field_label: '%Y-%m'},
            index_col=month_field_label)

    def construct_monthly_df(
            self, month_field_name, fields_to_query=None,
            session=None,
            filters=None, month_field_label='month'):
        return self.convert_monthly_query_to_df(
            self.construct_monthly_query(
                month_field_name,
                session=session,
                fields_to_query=fields_to_query,
                filters=filters,
                month_field_label=month_field_label
            ).subquery(),
            month_field_label=month_field_label
        )
