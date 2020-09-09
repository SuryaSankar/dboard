import math

import json

from flask import request, Response

from flask_sqlalchemy_booster.responses import as_json

from io import StringIO

from toolspy import merge, null_safe_type_cast, subdict, write_csv_file

from .utils import (
    convert_sqla_collection_items_to_dicts,
    get_queried_field_labels, sqla_sort)


QUERY_MODIFIERS = [
    'page', 'per_page', 'limit', 'offset', 'order_by', 'sort']


def fetch_query_modifiers_from_request():
    return subdict(
        request.args,
        QUERY_MODIFIERS
    )


def construct_query_modifiers(
        query_modifiers=None, allow_modification_via_requests=True):
    default_query_modifiers = {
        "page": None,
        "per_page": 20,
        "limit": None,
        "offset": None,
        "order_by": None,
        "sort": "asc",
    }
    if query_modifiers is None:
        query_modifiers = {}
    query_modifiers = subdict(query_modifiers, QUERY_MODIFIERS)
    query_modifiers = merge(
        default_query_modifiers, query_modifiers)
    if allow_modification_via_requests:
        query_modifiers = merge(
            query_modifiers, fetch_query_modifiers_from_request())
    for k in ['page', 'per_page', 'limit', 'offset']:
        if k in query_modifiers:
            query_modifiers[k] = null_safe_type_cast(
                int, query_modifiers.get(k))
    return query_modifiers


def apply_modifiers_on_sqla_query(
        q, page=None, per_page=20, limit=None, offset=None,
        order_by=None, sort='asc'):
    if order_by is not None:
        q = q.order_by(sqla_sort(sort)(order_by))
    if page:
        per_page = int(per_page)
        q = q.limit(per_page).offset((int(page) - 1) * per_page)
    elif limit and offset:
        q = q.limit(limit).offset(int(offset) - 1)
    return q


def construct_meta_dict_from_query(q, query_modifiers):
    meta = {
        "total_items": q.count(),
        "columns": get_queried_field_labels(q)
    }
    if query_modifiers.get("page"):
        meta["page"] = query_modifiers["page"]
        meta["per_page"] = query_modifiers.get("per_page")
        meta["total_pages"] = math.ceil(
            meta["total_items"] / meta["per_page"])
    return meta


def construct_list_of_dicts_from_query(
        q, query_modifiers=None, allow_modification_via_requests=True):
    query_modifiers = construct_query_modifiers(
        query_modifiers,
        allow_modification_via_requests=allow_modification_via_requests)
    q = apply_modifiers_on_sqla_query(q, **query_modifiers)
    return convert_sqla_collection_items_to_dicts(q.all())


def construct_json_response_from_query(
        q, query_modifiers=None, allow_modification_via_requests=True):

    query_modifiers = construct_query_modifiers(
        query_modifiers,
        allow_modification_via_requests=allow_modification_via_requests)
    meta = construct_meta_dict_from_query(q, query_modifiers)

    q = apply_modifiers_on_sqla_query(q, **query_modifiers)

    result = q.all()
    # q.session.remove()

    return as_json(
        convert_sqla_collection_items_to_dicts(
            result
        ),
        meta=meta,
        struct_key="data"
    )


def convert_csv_text_to_csv_response(csvtext):
    return Response(csvtext, mimetype="text/csv")


def construct_csv_response_from_query(
        q, query_modifiers=None, allow_modification_via_requests=True):
    cols = get_queried_field_labels(q)
    rows = construct_list_of_dicts_from_query(
        q, query_modifiers=query_modifiers,
        allow_modification_via_requests=allow_modification_via_requests)
    strfile = StringIO()
    write_csv_file(strfile, rows=rows, cols=cols)
    csv_content = strfile.getvalue().strip("\r\n")
    strfile.close()
    return convert_csv_text_to_csv_response(csv_content)


def fetch_filter_params(
        filter_params_schema=None, filter_params_arg='filter_params',
        convert_empty_string_to_none=True):
    filter_params = request.args.get(filter_params_arg)
    if filter_params and filter_params_schema:
        filter_params = json.loads(filter_params)
        for k, v in filter_params.items():
            if v == "":
                filter_params[k] = None
        filter_params = filter_params_schema().load(
            filter_params)
    return filter_params


def construct_response_from_query(
        q, json_query_modifiers=None, csv_query_modifiers=None,
        response_format=None):
    if response_format is None:
        response_format = request.args.get('format')
    if response_format == 'csv':
        return construct_csv_response_from_query(
            q, query_modifiers=csv_query_modifiers)
    elif response_format == 'dict':
        return construct_list_of_dicts_from_query(
            q, query_modifiers=json_query_modifiers)
    return construct_json_response_from_query(
        q, query_modifiers=json_query_modifiers)


def render_query_response(
        query_constructor, query_engine, db_base,
        json_query_modifiers=None,
        csv_query_modifiers=None, filter_params_schema=None,
        filter_params=None,
        response_format=None):
    if filter_params is None:
        filter_params = fetch_filter_params(
            filter_params_schema=filter_params_schema)
    session = query_engine.session()
    try:
        q = query_constructor(
            session, query_engine, db_base, filter_params=filter_params)
        response = construct_response_from_query(
            q, json_query_modifiers=json_query_modifiers,
            csv_query_modifiers=csv_query_modifiers,
            response_format=response_format)
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
    return response


def convert_error_to_json_response(e):
    response = e.get_response()
    response.data = json.dumps({
        "status": "failure",
        "error": {
            "code": e.code,
            "name": e.name,
            "description": e.description
        }
    })
    response.content_type = "application/json"
    return response


def register_query_endpoints(app_or_bp, registration_dict):
    """
    registration_dict = {
        "/daily-transactions": {
            "query_constructor": some_query_func,
            "filter_params_schema": SomeSchemaClass,
            "json_query_modifiers": {}
        }
    }
    """
    def construct_get_func(
            query_constructor, json_query_modifiers=None,
            csv_query_modifiers=None,
            filter_params_schema=None):
        def _get_func():
            return render_query_response(
                query_constructor, json_query_modifiers=json_query_modifiers,
                csv_query_modifiers=csv_query_modifiers,
                filter_params_schema=filter_params_schema)
        return _get_func

    for url, data in registration_dict.items():
        get_func = construct_get_func(
            data["query_constructor"],
            json_query_modifiers=data.get("json_query_modifiers"),
            csv_query_modifiers=data.get("csv_query_modifiers"),
            filter_params_schema=data.get("filter_params_schema")
        )
        app_or_bp.route(
            url, methods=['GET'], endpoint=url.strip("/").replace(
                "-", "_").replace("/", "_")
        )(get_func)

    return app_or_bp
