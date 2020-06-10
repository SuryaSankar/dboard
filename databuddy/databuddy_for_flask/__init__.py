from flask import Blueprint, request, url_for, render_template
import json
from toolspy import set_query_params
from .template_filters import register_template_filters
from .data_sources import (
    prepare_data_sources, construct_sqla_db_uri,
    sqla_db_info, sqla_query_builder, sqla_base)


def create_blueprint(
        app,
        blueprint_name="databuddy", 
        blueprint_url_prefix="/databuddy"):
    bp = Blueprint(
        blueprint_name, __name__,
        url_prefix=blueprint_url_prefix,
        template_folder="templates")
    return bp

def transformed_dict(
        d, keys_to_retain=None, keys_to_rename=None,
        skip_none_vals=True):
    result = {}
    for k, v in d.items():
        if v is None and skip_none_vals:
            continue
        if keys_to_retain is None or k in keys_to_retain:
            if keys_to_rename and k in keys_to_rename:
                key = keys_to_rename.get(k)
            else:
                key = k
            result[key] = v
    print("transformed_dict is ", result)
    return result


def render_table_layout(
        api_endpoint,
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
        api_url=set_query_params(
            url_for(api_endpoint),
            transformed_dict(
                request.args, 
                keys_to_retain=['target_db', table_filters_form_id],
                keys_to_rename={table_filters_form_id: "filter_params"}
            )
        ),
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
            nav_menu_items=None):
        self.pages_bp = None
        if app is not None:
            self.init_app(
                app, blueprint_name=blueprint_name,
                blueprint_url_prefix=blueprint_url_prefix,
                nav_menu_items=nav_menu_items)


    def init_app(
            self, app, blueprint_name="databuddy", 
            blueprint_url_prefix="/databuddy",
            nav_menu_items=None):
        '''Initalizes the application with the extension.
        :param app: The Flask application object.
        '''
        self.pages_bp = create_blueprint(
            app, blueprint_name=blueprint_name,
            blueprint_url_prefix=blueprint_url_prefix)
        app.register_blueprint(self.pages_bp)
        register_template_filters(app)
        prepare_data_sources(app.config["DATA_SOURCES"], app)

        @app.context_processor
        def inject_nav_menu_items():
            return dict(
                nav_menu_items=nav_menu_items
            )
