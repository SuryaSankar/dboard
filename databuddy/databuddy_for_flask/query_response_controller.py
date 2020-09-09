from flask import request
from .data_sources import sqla_base, sqla_query_builder
from ..response_generators import (
    fetch_filter_params, construct_response_from_query)


class QueryResponseController(object):

    class ParamSchema:
        pass

    def __init__(
            self, datasource_name=None, response_format=None,
            json_query_modifiers=None, csv_query_modifiers=None):
        self.datasource_name = datasource_name or self.get_datasource_name()
        self.response_format = response_format or self.get_response_format()
        self.json_query_modifiers = json_query_modifiers or self.get_json_query_modifiers()
        self.csv_query_modifiers = csv_query_modifiers or self.get_csv_query_modifiers()
        self.query_engine = sqla_query_builder(self.datasource_name)
        self.db_base = sqla_base(self.datasource_name)

    def get_datasource_name(self):
        raise NotImplementedError

    def get_response_format(self):
        return 'json'

    def get_json_query_modifiers(self):
        return None

    def get_csv_query_modifiers(self):
        return None

    def query(self, params=None):
        raise NotImplementedError

    def render_response(self):
        params = fetch_filter_params(
            filter_params_schema=self.ParamSchema)
        response_format = request.args.get('format') or self.response_format
        q = self.query(params=params)
        response = construct_response_from_query(
            q, json_query_modifiers=self.json_query_modifiers,
            csv_query_modifiers=self.csv_query_modifiers,
            response_format=response_format)
        return response
