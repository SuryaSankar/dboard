import pandas as pd

from .dash_utils import *
from .datetime_utils import *
from .formatters import *
from .function_utils import *
from sqlalchemy import asc, desc, func


def get_queried_field_labels(q):
    return [e._label_name for e in q._entities]


def sqla_sort(sort_order):
    return asc if sort_order == 'asc' else desc


def convert_sqla_collection_item_to_dict(item):
    return {f: getattr(item, f) for f in item._fields}


def convert_sqla_collection_items_to_dicts(collection):
    return [convert_sqla_collection_item_to_dict(item) for item in collection]


def groupby_result_to_pd_series(result):
    return pd.Series({k: v for k, v in result})


def null_safe_sum(col):
    return func.sum(func.ifnull(col, 0))
