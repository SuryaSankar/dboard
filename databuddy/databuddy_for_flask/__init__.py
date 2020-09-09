from flask import Blueprint, request, url_for, render_template
import json
from toolspy import set_query_params
from .template_filters import register_template_filters
from .data_sources import (
    prepare_data_sources, construct_sqla_db_uri,
    sqla_db_info, sqla_query_builder, sqla_base)
from .query_response_controller import QueryResponseController


def create_blueprint(
        app,
        blueprint_name="databuddy",
        blueprint_url_prefix="/databuddy"):
    bp = Blueprint(
        blueprint_name, __name__,
        url_prefix=blueprint_url_prefix,
        template_folder="templates")
    return bp


def render_table_layout(
        api_url,
        table_heading,
        table_filters,
        table_filters_form_id="filters",
        template_path="databuddy/table_layout.html",
        **kwargs):
    filter_params = request.args.get(table_filters_form_id)
    if filter_params:
        filter_params = json.loads(filter_params)
        for filter_field in table_filters:
            if filter_field['name'] in filter_params:
                filter_field['value'] = filter_params[filter_field['name']]

    return render_template(
        template_path,
        heading=table_heading,
        api_url=api_url,
        filters=table_filters,
        **kwargs
    )


class DatabuddyForFlask(object):
    """
    Doc string.
    """

    def __init__(
            self, app, blueprint_name="databuddy",
            blueprint_url_prefix="/databuddy",
            nav_menu_items=None, engine_kwargs=None):
        self.pages_bp = None
        if app is not None:
            self.init_app(
                app, blueprint_name=blueprint_name,
                blueprint_url_prefix=blueprint_url_prefix,
                nav_menu_items=nav_menu_items,
                engine_kwargs=engine_kwargs)

    def init_app(
            self, app, blueprint_name="databuddy",
            blueprint_url_prefix="/databuddy",
            nav_menu_items=None, engine_kwargs=None):
        '''Initalizes the application with the extension.
        :param app: The Flask application object.
        '''
        self.pages_bp = create_blueprint(
            app, blueprint_name=blueprint_name,
            blueprint_url_prefix=blueprint_url_prefix)
        app.register_blueprint(self.pages_bp)
        register_template_filters(app)
        prepare_data_sources(
            app.config["DATA_SOURCES"], app, engine_kwargs=engine_kwargs)

        @app.context_processor
        def inject_nav_menu_items():
            return dict(
                nav_menu_items=nav_menu_items
            )
