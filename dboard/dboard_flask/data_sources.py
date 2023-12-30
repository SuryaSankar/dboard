from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData
from flask import current_app
from toolspy import merge, fetch_nested_key_from_dict
import pandas as pd

def construct_sqla_db_uri(db_dict):
    return "{db_type}://{db_user}:{db_password}@{db_server}/{db_name}".format(
        **db_dict)

def setup_db(conn_string):
    db_engine = create_engine(conn_string)
    db_metadata = MetaData()
    db_metadata.reflect(bind=db_engine)
    return db_engine, db_metadata

class DBStore:
    def __init__(self, conn_string):
        self.conn_string = conn_string
        self.engine, self.metadata = setup_db(conn_string)
        self.tables = self.metadata.tables
    
    def sqltodf(self, stmt, index_col=None):
        return pd.read_sql(stmt, self.engine, index_col=index_col)


def prepare_data_sources(data_sources, app, engine_kwargs=None):
    for db_name, db_dict in data_sources.items():
        engine_kwargs_for_db = merge(
            fetch_nested_key_from_dict(engine_kwargs, '*') or {},
            fetch_nested_key_from_dict(engine_kwargs, db_name) or {}
        )
        db_dict["sqla_db_uri"] = construct_sqla_db_uri(db_dict)
        db_dict["db_store"] = DBStore(db_dict["sqla_db_uri"])
        db_dict["engine"] = create_engine(
            db_dict["sqla_db_uri"], **engine_kwargs_for_db)
        if db_dict.get("automap_tables"):
            db_dict["metadata"] = MetaData()
            db_dict["metadata"].reflect(db_dict["engine"])
        elif db_dict.get("automap_orm"):
            db_dict["base"] = automap_base()
            db_dict["base"].prepare(db_dict["engine"], reflect=True)


def sqla_db_info(db_name):
    return current_app.config["DATA_SOURCES"][db_name]

def sqla_base(db_name):
    return sqla_db_info(db_name)["base"]

def get_db_store(db_name):
    return sqla_db_info(db_name)["db_store"]
