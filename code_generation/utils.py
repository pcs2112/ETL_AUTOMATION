import sys
import datetime


def set_nullable(column, parts):
    if column['is_nullable'] == 0:
        parts.append('NOT NULL')


def set_data_type(column, parts):
    parts.append(f"[{column['data_type']}]")


def set_datetime(column, parts):
    parts.append(f"[{column['data_type']}]")


def set_varchar(column, parts):
    parts.append(f"[{column['data_type']}] ({column['max_length']})")


def set_decimal(column, parts):
    parts.append(f"[{column['data_type']}] ({column['max_length']},{column['precision']})")


def set_numeric(column, parts):
    parts.append(f"[{column['data_type']}] ({column['max_length']},{column['precision']})")


def get_table_definition(table_definition):
    module = sys.modules[__name__]
    columns = []

    for column in table_definition:
        parts = [column['column_name']]

        try:
            func = getattr(module, 'set_' + column['data_type'])
            func(column, parts)
        except:
            set_data_type(column, parts)

        set_nullable(column, parts)
        columns.append(' '.join([str(part) for part in parts]))

    return columns


def get_column_names(table_definition):
    columns = []
    for column in table_definition:
        columns.append(column['column_name'])

    return columns


def get_identity_column(table_definition):
    for column in table_definition:
        if column['is_identity'] == 1:
            return column

    return None


def get_current_timestamp(format_str="%Y-%m-%d %H:%M"):
    now = datetime.datetime.now()
    return now.strftime(format_str)
