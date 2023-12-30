def format_as_currency(v):
    return "Rs. {:,.2f}".format(v)


def format_as_percentage(v):
    return "{:.2f} %".format(v)


def format_value(value, field_type=None):
    if field_type == 'currency':
        return format_as_currency(value)
    if field_type == 'percentage':
        return format_as_percentage(value)
    return value
