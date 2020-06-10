from toolspy import set_query_params


def format_datetime(dt, format_string):
    if dt is None:
        return ""
    return dt.strftime(format_string)


template_filters = {
    "set_query_params": set_query_params,
    "format_datetime": format_datetime
}


def register_template_filters(app):
    for filter_name, filter in template_filters.items():
        app.jinja_env.filters[filter_name] = filter