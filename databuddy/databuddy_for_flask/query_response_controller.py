from flask import request
from .data_sources import sqla_base, sqla_query_builder
from ..response_generators import (
    fetch_filter_params, construct_response_from_query)


class QueryResponseController(object):

    class ParamSchema:
        pass

    def __init__(
            self, datasource_name=None, response_format=None,
            params=None):
        self.datasource_name = datasource_name or self.get_datasource_name()
        self.response_format = response_format or self.get_response_format()
        self.params = params or fetch_filter_params(
            filter_params_schema=self.ParamSchema)
        self.query_engine = sqla_query_builder(self.datasource_name)
        self.db_base = sqla_base(self.datasource_name)

    def get_datasource_name(self):
        raise NotImplementedError

    def get_response_format(self):
        return request.args.get('format') or 'json'

    def query(self, params=None):
        raise NotImplementedError

    def construct_response(self, q):
        return construct_response_from_query(
            q, response_format=self.response_format)

    def render_response(self):
        return self.construct_response(self.query())
