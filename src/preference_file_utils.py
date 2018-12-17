import os
import json
import src.utils
import src.code_gen.utils
import src.db_utils
from src.config import get_config
from src.excel_utils import read_workbook, read_workbook_data
from src.mssql_connection import init_db, close


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


def create_json_preference_file(file_name, contents):
	"""
	Writes and creates a JSON preference file.
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
		'SOURCE_EXCLUDED_COLUMNS',
		'TARGET_TABLE_EXTRA_KEY_COLUMNS',
		'TARGET_TABLE_EXTRA_COLUMNS',
		'UPDATE_MATCH_CHECK_COLUMNS'
	]

	# Validate the string args
	for str_arg in str_args:
		if str_arg not in config or not isinstance(config[str_arg], str):
			raise ValueError(f"{str_arg} must be a string.")

	# Validate the int args
	for int_arg in int_args:
		if int_arg not in config or not isinstance(config[int_arg], int):
			raise ValueError(f"{int_arg} must be an integer.")

	# Validate the array args
	for arr_arg in arr_args:
		if arr_arg not in config or not isinstance(config[arr_arg], list):
			raise ValueError(f"{arr_arg} must be an array.")

	# Validate the search column
	search_column_name = config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']
	if search_column_name != '' and not src.code_gen.utils.get_column_exists(table_definition, search_column_name):
		raise ValueError(f"SOURCE_TABLE_SEARCH_COLUMN: \"{search_column_name}\" is an invalid column.")

	# Validate the update check columns
	if len(config['TARGET_TABLE_EXTRA_KEY_COLUMNS']) > 0:
		for column_name in config['TARGET_TABLE_EXTRA_KEY_COLUMNS']:
			if not src.code_gen.utils.get_column_exists(table_definition, column_name):
				raise ValueError(f"TARGET_TABLE_EXTRA_KEY_COLUMNS: \"{column_name}\" is an invalid column.")

	# Validate the update check columns
	if len(config['UPDATE_MATCH_CHECK_COLUMNS']) > 0:
		for column_name in config['UPDATE_MATCH_CHECK_COLUMNS']:
			if not src.code_gen.utils.get_column_exists(table_definition, column_name):
				raise ValueError(f"UPDATE_MATCH_CHECK_COLUMNS: \"{column_name}\" is an invalid column.")


def get_excel_preference_file_data(file_name):
	"""
	Returns a list of dicts containing the data for each preference file.
	:param str file_name:
	:return list
	"""
	wb = read_workbook(get_configuration_file_path(file_name))
	in_data = read_workbook_data(wb)
	out_data = []

	for i in range(len(in_data)):
		obj = in_data[i]

		# Get the target table name
		target_table = src.code_gen.utils.get_target_table_name(obj['SOURCE_TABLE'], obj['TARGET_TABLE'])

		# Get the stored procedure name
		sp_name = src.code_gen.utils.get_sp_name(target_table, obj['STORED_PROCEDURE_NAME'])

		# Get the target table extra columns
		target_table_extra_cols_list =  src.utils.split_string(obj['TARGET_TABLE_EXTRA_COLUMNS'], '|')
		target_table_extra_cols = []

		for col in target_table_extra_cols_list:
			props = src.utils.split_string(col, ';')
			if len(props) > 0:
				target_table_extra_cols.append({
					'column_name': props[0],
					'data_type': props[1],
					'value': props[2]
				})

		config = {
			'SOURCE_SERVER': obj['SOURCE_SERVER'],
			'SOURCE_DATABASE': obj['SOURCE_DATABASE'],
			'SOURCE_SCHEMA': obj['SOURCE_SCHEMA'],
			'SOURCE_TABLE': obj['SOURCE_TABLE'],
			'SOURCE_DATA_MART': obj['SOURCE_DATA_MART'],
			'SOURCE_TABLE_SEARCH_COLUMN': {
				'column_name': obj['SOURCE_TABLE_SEARCH_COLUMN_NAME'],
				'is_utc': obj['SOURCE_TABLE_SEARCH_COLUMN_IS_UTC'] == 'true'
			},
			'SOURCE_TABLE_SEARCH_CONDITION': obj['SOURCE_TABLE_SEARCH_CONDITION'],
			'SOURCE_TABLE_PRIMARY_KEY': obj['SOURCE_TABLE_PRIMARY_KEY'],
			'SOURCE_EXCLUDED_COLUMNS': src.utils.split_string(obj['SOURCE_EXCLUDED_COLUMNS'], '|'),
			'TARGET_SERVER': obj['TARGET_SERVER'],
			'TARGET_DATABASE': obj['TARGET_DATABASE'],
			'TARGET_SCHEMA': obj['TARGET_SCHEMA'],
			'TARGET_TABLE': target_table,
			'TARGET_TABLE_EXTRA_KEY_COLUMNS': src.utils.split_string(obj['TARGET_TABLE_EXTRA_KEY_COLUMNS'], '|'),
			'TARGET_TABLE_EXTRA_COLUMNS': target_table_extra_cols,
			'DATA_PARTITION_FUNCTION': obj['DATA_PARTITION_FUNCTION'],
			'DATA_PARTITION_COLUMN': obj['DATA_PARTITION_COLUMN'],
			'INDEX_PARTITION_FUNCTION': obj['INDEX_PARTITION_FUNCTION'],
			'INDEX_PARTITION_COLUMN': obj['INDEX_PARTITION_COLUMN'],
			'STORED_PROCEDURE_SCHEMA': obj['STORED_PROCEDURE_SCHEMA'],
			'STORED_PROCEDURE_NAME': sp_name,
			'UPDATE_MATCH_CHECK_COLUMNS': src.utils.split_string(obj['UPDATE_MATCH_CHECK_COLUMNS'], '|'),
			'MIN_CALL_DURATION_MINUTES': int(obj['MIN_CALL_DURATION_MINUTES']),
			'MAX_CALL_DURATION_MINUTES': int(obj['MAX_CALL_DURATION_MINUTES']),
			'ETL_PRIORITY': int(obj['ETL_PRIORITY']),
			'SOURCE_TYPE': obj['SOURCE_TYPE']
		}

		out_data.append(config)

	return out_data


def create_excel_preference_file(in_data):
	for i, row in enumerate(in_data):
		create_excel_preference_file_row(row)


def create_excel_preference_file_row(pref_config):
	# Init DB connection
	db_config = {
		'DB_SERVER': pref_config['SOURCE_SERVER'],
		'DB_NAME': pref_config['SOURCE_DATABASE'],
		'DB_USER': '',
		'DB_PASSWORD': '',
		'DB_DRIVER': 'SQL Server',
		'DB_TRUSTED_CONNECTION': 1
	}

	init_db(db_config)

	# Get the table definition from the specified config
	table_definition = src.db_utils.get_table_definition(
		pref_config['SOURCE_DATABASE'],
		pref_config['SOURCE_TABLE'],
		'' if pref_config['SOURCE_SERVER'] == 'localhost' else pref_config['SOURCE_SERVER'],
		pref_config['SOURCE_EXCLUDED_COLUMNS']
	)

	try:
		validate_preference_file(table_definition, pref_config)
	except ValueError as e:
		print(str(e))

	close()
