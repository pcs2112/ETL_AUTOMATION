import os
from src.config import get_config
from src.mssql_connection import fetch_rows


def get_table_definition_from_source(db_name, table_name, server_name='', excluded_columns=()):
	"""
	Returns the table definition.
	:param str db_name: Database name
	:param str table_name: Database table name
	:param str server_name: Server name
	:param list excluded_columns: Columns to exclude
	:return list
	"""
	server_name = f'[{server_name}].' if server_name else ''

	sql = ("SELECT T.name AS TABLE_NAME, C.name AS COLUMN_NAME, P.name AS DATA_TYPE, "
			"P.max_length AS SIZE, CAST(P.precision AS VARCHAR) + '/' + CAST(P.scale AS VARCHAR) AS PRECISION_SCALE, "
			"c.* FROM {0}[{1}].sys.objects AS T JOIN {0}[{1}].sys.columns AS C ON T.object_id = C.object_id "
			"JOIN {0}[{1}].sys.types AS P ON C.system_type_id = P.system_type_id WHERE  T.type_desc = 'USER_TABLE' "
			"and T.name = ? and P.name != 'timestamp' order by column_id asc").format(server_name, db_name, table_name)

	columns = fetch_rows(sql, [table_name])

	default_columns = {
		'ID': 1,
		'INSERT_DTTM': 1,
		'UPDATE_DTTM': 1,
		'LST_MOD_USER': 1,
		'MSTR_LOAD_ID': 1,
		'ACTIVE_FLAG': 1
	}

	target_table_column_prefix = get_config()['TARGET_TABLE_COLUMN_PREFIX']
	out_columns = {}

	for column in columns:
		if column['column_name'].upper() in default_columns:
			column['source_table_column_name'] = target_table_column_prefix + column['column_name']
		else:
			column['source_table_column_name'] = column['column_name']

		out_columns[column['column_name'].upper()] = column

	if len(excluded_columns) > 0:
		for excluded_column in excluded_columns:
			out_columns.pop(excluded_column)

	return out_columns


def get_base_sql_code(file_name):
	"""
	Returns the file contents in a string.
	:param str file_name:
	:return: str
	"""
	file_path = os.path.join(get_config()['CODE_GENERATION_IN_DIR'], file_name)
	with open(file_path) as fp:
		contents = fp.read()

	return contents


def create_sql_file(file_name, sql_contents):
	"""
	Writes and creates a sql file.
	:param str file_name: File name
	:param str sql_contents: The file's SQL contents
	:return: str New file path
	"""
	file_path = os.path.join(get_config()['ETL_CONFIG_OUT_DIR'], file_name)
	with open(file_path, "w+") as fp:
		fp.write(sql_contents)

	return file_path


def split_string(value, delimiter=','):
	""" Splits the specified string and returns an array containing each part. """
	tmp_parts = value.split(delimiter)
	parts = []
	if len(tmp_parts) > 0:
		for tmp_part in tmp_parts:
			tmp_part = tmp_part.strip()
			if tmp_part != '':
				parts.append(tmp_part)

	return parts
