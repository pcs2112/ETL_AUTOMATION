import os
import json
import src.db_utils
import src.code_gen.utils
import src.excel_utils
import src.utils
from src.config import get_config
from src.excel_utils import read_workbook, read_workbook_data
from src.mssql_connection import init_db, close
from src.exceptions import SearchColumnNoIndex, SearchColumnNotFound, SearchColumnInvalidValue


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
	Creates a JSON preference file from a text file.
	:param str file_name: File name
	:param str contents: The file's contents
	:return: str New file path
	"""
	file_path = os.path.join(get_config()['ETL_CONFIG_IN_DIR'], file_name)
	with open(file_path, "w+") as fp:
		fp.write(contents)

	return file_path


def create_json_preference_files(file_name):
	"""
	Creates the JSON preference files from an excel file.
	:param str file_name: File name
	:return: str New file path
	"""
	data = get_excel_preference_file_data(file_name)
	files = []
	for config in data:
		files.append(create_json_preference_file(
			f"C8_{config['STORED_PROCEDURE_NAME']}.json",
			json.dumps(config, indent=4)
		))

	return files


def create_excel_preference_file(in_filename):
	""" Creates the final verified excel preference file. """
	data = get_excel_preference_file_data(in_filename)
	rows = []
	header = [
		'SOURCE_SERVER',
		'SOURCE_DATABASE',
		'SOURCE_SCHEMA',
		'SOURCE_TABLE',
		'SOURCE_DATA_MART',
		'SOURCE_TABLE_SEARCH_COLUMN_NAME',
		'SOURCE_TABLE_SEARCH_COLUMN_IS_UTC',
		'SOURCE_TABLE_SEARCH_CONDITION',
		'SOURCE_TABLE_PRIMARY_KEY',
		'SOURCE_EXCLUDED_COLUMNS',
		'TARGET_SERVER',
		'TARGET_DATABASE',
		'TARGET_SCHEMA',
		'TARGET_TABLE',
		'TARGET_TABLE_EXTRA_KEY_COLUMNS',
		'TARGET_TABLE_EXTRA_COLUMNS',
		'DATA_PARTITION_FUNCTION',
		'DATA_PARTITION_COLUMN',
		'INDEX_PARTITION_FUNCTION',
		'INDEX_PARTITION_COLUMN',
		'STORED_PROCEDURE_SCHEMA',
		'STORED_PROCEDURE_NAME',
		'UPDATE_MATCH_CHECK_COLUMNS',
		'MIN_CALL_DURATION_MINUTES',
		'MAX_CALL_DURATION_MINUTES',
		'ETL_PRIORITY',
		'SOURCE_TYPE',
		'SOURCE_TABLE_SEARCH_COLUMN_EXISTS',
		'SOURCE_TABLE_SEARCH_COLUMN_INDEX_EXISTS',
		'ROW_COUNT',
		'ROW_MIN_DATE',
		'ROW_MAX_DATE',
		'ROW_MONTH_COUNT',
		'ERROR_MESSAGE'
	]

	rows.append(header)

	for i, row in enumerate(data):
		rows.append(create_excel_preference_file_row(row))

	out_filename, file_extension = os.path.splitext(in_filename)
	out_filename = out_filename + '_final' + file_extension

	src.excel_utils.write_workbook_data(out_filename, ['ETL_STORED_PROCEDURES'], rows)
	return out_filename


def get_excel_preference_file_data(file_name):
	"""
	Returns a list of dicts containing the data for each preference file.
	:param str file_name:
	:return list
	"""
	wb = read_workbook(file_name)
	in_data = read_workbook_data(wb)
	out_data = []

	for i in range(len(in_data)):
		obj = in_data[i]

		# Get the target table name
		target_table = src.code_gen.utils.get_target_table_name(obj['SOURCE_TABLE'], obj['TARGET_TABLE'])

		# Get the stored procedure name
		sp_name = src.code_gen.utils.get_sp_name(target_table, obj['STORED_PROCEDURE_NAME'])

		# Get the target table extra columns
		target_table_extra_cols_list = src.utils.split_string(obj['TARGET_TABLE_EXTRA_COLUMNS'], '|')
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


def create_excel_preference_file_row(pref_config):
	""" Uses the specified preference config to create a row of data for the excel file. """
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
	err = ''
	search_column_name = ''
	search_column_exists = True
	search_column_has_index = True
	counts = {
		'count': '',
		'min_value': '',
		'max_value': '',
		'month_cnt': ''
	}

	try:
		table_definition = src.db_utils.get_table_definition(
			pref_config['SOURCE_DATABASE'],
			pref_config['SOURCE_TABLE'],
			pref_config['SOURCE_SERVER'],
			pref_config['SOURCE_EXCLUDED_COLUMNS']
		)

		search_column_name = pref_config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']

		validate_preference_file_config(pref_config, table_definition)

		try:
			counts = src.db_utils.get_record_counts(
				pref_config['SOURCE_SCHEMA'], pref_config['SOURCE_TABLE'], search_column_name
			)
		except Exception as e:
			raise SearchColumnNoIndex(str(e))
	except SearchColumnInvalidValue as e:
		err = str(e)
		search_column_exists = False
		search_column_has_index = False
	except SearchColumnNotFound as e:
		err = str(e)
		search_column_exists = False
		search_column_has_index = False
	except SearchColumnNoIndex as e:
		err = str(e)
		search_column_exists = False
		search_column_has_index = False
	except Exception as e:
		err = str(e)

	close()

	row = [
		pref_config['SOURCE_SERVER'],
		pref_config['SOURCE_DATABASE'],
		pref_config['SOURCE_SCHEMA'],
		pref_config['SOURCE_TABLE'],
		pref_config['SOURCE_DATA_MART'],
		search_column_name,
		'true' if search_column_exists and pref_config['SOURCE_TABLE_SEARCH_COLUMN']['is_utc'] else 'false',
		pref_config['SOURCE_TABLE_SEARCH_CONDITION'],
		pref_config['SOURCE_TABLE_PRIMARY_KEY'],
		pref_config['SOURCE_EXCLUDED_COLUMNS'],
		pref_config['TARGET_SERVER'],
		pref_config['TARGET_DATABASE'],
		pref_config['TARGET_SCHEMA'],
		pref_config['TARGET_TABLE'],
		src.utils.serialize_list_for_excel(pref_config['TARGET_TABLE_EXTRA_KEY_COLUMNS']),
		src.utils.serialize_list_for_excel(pref_config['TARGET_TABLE_EXTRA_COLUMNS']),
		pref_config['DATA_PARTITION_FUNCTION'],
		pref_config['DATA_PARTITION_COLUMN'],
		pref_config['INDEX_PARTITION_FUNCTION'],
		pref_config['INDEX_PARTITION_COLUMN'],
		pref_config['STORED_PROCEDURE_SCHEMA'],
		pref_config['STORED_PROCEDURE_NAME'],
		src.utils.serialize_list_for_excel(pref_config['UPDATE_MATCH_CHECK_COLUMNS']),
		pref_config['MIN_CALL_DURATION_MINUTES'],
		pref_config['MAX_CALL_DURATION_MINUTES'],
		pref_config['ETL_PRIORITY'],
		pref_config['SOURCE_TYPE'],
		'true' if search_column_exists else 'false',
		'true' if search_column_has_index else 'false',
		counts['count'],
		counts['min_value'],
		counts['max_value'],
		counts['month_cnt'],
		err
	]

	return row


def validate_preference_file_config(config, table_definition):
	"""
	Validates the specified preference file configuration.
	:param dict config: Preference file configuration
	:param list table_definition: Table definition
	"""
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
	search_column = config['SOURCE_TABLE_SEARCH_COLUMN']
	if not isinstance(search_column, dict) \
			or not hasattr(search_column, 'column_name') or not hasattr(search_column, 'is_utc'):
		raise SearchColumnInvalidValue(
			'SOURCE_TABLE_SEARCH_COLUMN must be a dictionary with the column_name and is_utc properties.'
		)

	search_column_name = search_column['column_name']
	if search_column_name == '':
		raise SearchColumnInvalidValue('SOURCE_TABLE_SEARCH_COLUMN: column name can\'t be empty.')

	if not src.code_gen.utils.get_column_exists(table_definition, search_column_name):
		raise SearchColumnNotFound(f"SOURCE_TABLE_SEARCH_COLUMN: \"{search_column_name}\" is an invalid column.")

	# Check the index exists
	try:
		search_column_index_exists = src.db_utils.column_index_exists(
			config['SOURCE_SCHEMA'],
			config['SOURCE_TABLE'],
			search_column_name
		)
	except Exception as e:
		raise SearchColumnNoIndex(f"SOURCE_TABLE_SEARCH_COLUMN: {str(e)}")

	if not search_column_index_exists:
		raise ValueError(f"SOURCE_TABLE_SEARCH_COLUMN: \"{search_column_name}\" does not have an index.")

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
