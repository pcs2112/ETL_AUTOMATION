import sys
import datetime


def set_nullable(column, parts):
	if column['is_nullable'] == 0:
		parts.append('NOT NULL')


def set_data_type(column, parts):
	parts.append(f"[{column['data_type']}]")


def set_datetime(column, parts):
	parts.append(f"[{column['data_type']}]")


def set_char(column, parts):
	return set_varchar(column, parts)
	

def set_varchar(column, parts):
	max_length = 'max' if column['max_length'] < 1 else column['max_length']
	parts.append(f"[{column['data_type']}] ({max_length})")


def set_decimal(column, parts):
	return set_numeric(column, parts)


def set_money(column, parts):
	return set_numeric(column, parts)


def set_numeric(column, parts):
	parts.append(f"[{column['data_type']}] ({column['precision']},{column['max_length']})")


def get_table_definition(table_definition):
	module = sys.modules[__name__]
	columns = []

	for key, column in table_definition.items():
		parts = [key]

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
	for key, column in table_definition.items():
		columns.append(key)

	return columns


def get_identity_column(table_definition):
	for key, column in table_definition.items():
		if column['is_identity'] == 1:
			return column

	return None


def get_current_timestamp(format_str="%Y-%m-%d %H:%M"):
	now = datetime.datetime.now()
	return now.strftime(format_str)


def get_target_table_name(source_table_name, target_table_name):
	return target_table_name.replace('<SOURCE_TABLE>', source_table_name)


def get_sp_name(target_table_name, sp_name):
	return sp_name.replace('<TARGET_TABLE>', target_table_name)


def get_column_exists(table_definition, column_name):
	return column_name.upper() in table_definition
