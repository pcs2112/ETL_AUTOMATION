import os
import json
from config import get_config
from mssql_connection import fetch_rows
from code_generation.utils import get_column_exists


def get_table_definition_from_source(db_name, table_name, server_name=''):
	"""
	Returns the table definition.
	:param str db_name: Database name
	:param str table_name: Database table name
	:param str server_name: Server name
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

	out_columns = {}

	for column in columns:
		if column['column_name'].upper() in default_columns:
			column['column_name'] = 'S_' + column['column_name']

		out_columns[column['column_name'].upper()] = column

	return out_columns


def get_configuration_file_path(file_name):
	"""
	Returns the path for the specified configuration file.
	:param str file_name:
	:return: str
	"""
	file_path = os.path.join(get_config()['ETL_CONFIG_IN_DIR'], file_name)
	if os.path.exists(file_path) is False:
		raise FileExistsError(f"{file_name} is an invalid file.")

	return file_path


def get_configuration_file(file_name):
	"""
	Returns the data for the specified configuration file.
	:param str file_name:
	:return: str
	"""
	file_path = get_configuration_file_path(file_name)

	with open(file_path) as fp:
		contents = fp.read()

	return contents


def get_configuration_from_preference_file(file_name):
	"""
	Returns the staging table configuration from a preference file.
	:param str file_name:
	:return: dict
	"""
	return json.loads(get_configuration_file(file_name))


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


def create_preference_file(file_name, contents):
	"""
	Writes and creates a preference file.
	:param str file_name: File name
	:param str contents: The file's contents
	:return: str New file path
	"""
	file_path = os.path.join(get_config()['ETL_CONFIG_IN_DIR'], file_name)
	with open(file_path, "w+") as fp:
		fp.write(contents)

	return file_path


def validate_preference_file(table_definition, config):
	# Validate string arguments
	str_args = [
		'SOURCE_SERVER',
		'SOURCE_DATABASE',
		'SOURCE_SCHEMA',
		'SOURCE_TABLE',
		'SOURCE_DATA_MART',
		'SOURCE_TABLE_SEARCH_CONDITION',
		'SOURCE_TABLE_PRIMARY_KEY',
		'TARGET_SERVER',
		'TARGET_DATABASE',
		'TARGET_SCHEMA',
		'TARGET_TABLE',
		'DATA_PARTITION_FUNCTION',
		'DATA_PARTITION_COLUMN',
		'INDEX_PARTITION_FUNCTION',
		'INDEX_PARTITION_COLUMN',
		'STORED_PROCEDURE_NAME',
		'SOURCE_TYPE'
	]

	int_args = [
		'MIN_CALL_DURATION_MINUTES',
		'MAX_CALL_DURATION_MINUTES',
		'ETL_PRIORITY'
	]

	arr_args = [
		'TARGET_TABLE_EXTRA_KEY_COLUMNS',
		'TARGET_TABLE_EXTRA_COLUMNS',
		'UPDATE_MATCH_CHECK_COLUMNS'
	]

	for str_arg in str_args:
		if str_arg not in config or not isinstance(config[str_arg], str):
			raise ValueError(f"{str_arg} must be a string.")

	for int_arg in int_args:
		if int_arg not in config or not isinstance(config[int_arg], int):
			raise ValueError(f"{int_arg} must be an integer.")

	for arr_arg in arr_args:
		if arr_arg not in config or not isinstance(config[arr_arg], list):
			raise ValueError(f"{arr_arg} must be an array.")

	search_column_name = config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']

	if search_column_name != '' and not get_column_exists(table_definition, search_column_name):
		raise ValueError(f"SOURCE_TABLE_SEARCH_COLUMN = \"{search_column_name}\" is an invalid column.")


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
