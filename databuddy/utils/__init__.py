import pandas as pd

from .datetime_utils import *
from .formatters import *
from .function_utils import *
from sqlalchemy import asc, desc, func
import sqlalchemy
from sqlalchemy.orm import class_mapper, query


def get_queried_field_labels(q):
    if len(q._entities) == 0 and isinstance(
            q._entities[0], query._MapperEntity):
        # If the query is like query(ModelName)
        return q._entities[0].mapper.columns.keys()
    # If the query is like query(Model1.col1, Model1.col2)
    return [e._label_name for e in q._entities]


def sqla_sort(sort_order):
    return asc if sort_order == 'asc' else desc


def convert_sqla_collection_item_to_dict(item):
    if hasattr(item, '_asdict'):
        return item._asdict()
    return {f: getattr(item, f) for f in class_mapper(
        type(item)).columns.keys()}


def convert_sqla_collection_items_to_dicts(collection):
    return [convert_sqla_collection_item_to_dict(item) for item in collection]


def groupby_result_to_pd_series(result):
    return pd.Series({k: v for k, v in result})


def null_safe_sum(col):
    return func.sum(func.ifnull(col, 0))

