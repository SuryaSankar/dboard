from flask import request
from ..response_generators import fetch_filter_params, construct_response_from_df


class DfResponseController(object):

    class ParamSchema:
        pass

    def __init__(
            self, response_format=None, params=None):
        self.response_format = response_format or self.get_response_format()
        self.params = params or fetch_filter_params(
            filter_params_schema=self.ParamSchema)

    def get_response_format(self):
        return request.args.get('format') or 'json'

    def get_df(self):
        raise NotImplementedError

    def render_response(self):
        return construct_response_from_df(self.get_df())