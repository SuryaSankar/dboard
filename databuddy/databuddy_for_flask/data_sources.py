from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData
from ..databuddy import SqlaQueryBuilder
from flask import current_app
from toolspy import merge, fetch_nested_key_from_dict


def construct_sqla_db_uri(db_dict):
    return "{db_type}://{db_user}:{db_password}@{db_server}/{db_name}".format(
        **db_dict)


def prepare_data_sources(data_sources, app, engine_kwargs=None):
    for db_name, db_dict in data_sources["sqla_compatible_dbs"].items():
        engine_kwargs_for_db = merge(
            fetch_nested_key_from_dict(engine_kwargs, '*') or {},
            fetch_nested_key_from_dict(engine_kwargs, db_name) or {}
        )
        db_dict["sqla_db_uri"] = construct_sqla_db_uri(db_dict)
        db_dict["engine"] = create_engine(
            db_dict["sqla_db_uri"], **engine_kwargs_for_db)
        if db_dict.get("automap_tables"):
            db_dict["metadata"] = MetaData()
            db_dict["metadata"].reflect(db_dict["engine"])
        elif db_dict.get("automap_orm"):
            db_dict["base"] = automap_base()
            db_dict["base"].prepare(db_dict["engine"], reflect=True)

        db_dict["query_builder"] = SqlaQueryBuilder(
            db_dict["engine"],
            flask_app=app,
            timedelta_mins_from_utc=app.config["TIMEDELTA_MINS"]
        )


def sqla_db_info(db_name):
    return current_app.config["DATA_SOURCES"]["sqla_compatible_dbs"][db_name]


def sqla_query_builder(db_name):
    return sqla_db_info(db_name)["query_builder"]


def sqla_base(db_name):
    return sqla_db_info(db_name)["base"]
