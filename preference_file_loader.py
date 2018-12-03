import xlrd
import json
from utils import get_configuration_file_path, create_preference_file, split_string


def get_workbook(file_name):
	""" Returns the workbook instance for the specified file. """
	return xlrd.open_workbook(get_configuration_file_path(file_name))


def get_workbook_columns(wb, sheet_index=0):
	""" Returns a list of columns for the specified workbook sheet. """
	sheet = wb.sheet_by_index(sheet_index)
	columns = []

	for i in range(sheet.ncols):
		columns.append(sheet.cell(0, i).value)

	return columns


def get_workbook_data(wb, sheet_index=0):
	"""
	Returns the specified workbook sheet's data as an array of objects
	using the header columns as the keys
	"""
	header_columns = get_workbook_columns(wb, sheet_index)
	sheet = wb.sheet_by_index(sheet_index)
	num_columns = sheet.ncols
	num_rows = sheet.nrows
	data = []

	for row in range(1, num_rows):
		obj = {}
		for col in range(num_columns):
			obj[header_columns[col]] = sheet.cell(row, col).value

		data.append(obj)

	return data


def create_preference_files(file_name):
	"""
	Creates the json preference files using the data in the
	specified excel file.
	:param str file_name:
	:return list
	"""
	wb = get_workbook(file_name)
	data = get_workbook_data(wb)
	files = []

	for i in range(len(data)):
		obj = data[i]

		# Get the stored procedure name
		sp_name = obj['STORED_PROCEDURE_NAME'].replace('<TARGET_TABLE>', obj['TARGET_TABLE'])

		# Get the target table extra columns
		target_table_extra_cols_list = split_string(obj['TARGET_TABLE_EXTRA_COLUMNS'], '|')
		target_table_extra_cols = []

		for col in target_table_extra_cols_list:
			props = split_string(col, ';')
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
			'TARGET_SERVER': obj['TARGET_SERVER'],
			'TARGET_DATABASE': obj['TARGET_DATABASE'],
			'TARGET_SCHEMA': obj['TARGET_SCHEMA'],
			'TARGET_TABLE': obj['TARGET_TABLE'],
			'TARGET_TABLE_EXTRA_KEY_COLUMNS': split_string(obj['TARGET_TABLE_EXTRA_KEY_COLUMNS'], '|'),
			'TARGET_TABLE_EXTRA_COLUMNS': target_table_extra_cols,
			'DATA_PARTITION_FUNCTION': obj['DATA_PARTITION_FUNCTION'],
			'DATA_PARTITION_COLUMN': obj['DATA_PARTITION_COLUMN'],
			'INDEX_PARTITION_FUNCTION': obj['INDEX_PARTITION_FUNCTION'],
			'INDEX_PARTITION_COLUMN': obj['INDEX_PARTITION_COLUMN'],
			'STORED_PROCEDURE_NAME': sp_name,
			'UPDATE_MATCH_CHECK_COLUMNS': split_string(obj['UPDATE_MATCH_CHECK_COLUMNS'], '|'),
			'MIN_CALL_DURATION_MINUTES': int(obj['MIN_CALL_DURATION_MINUTES']),
			'MAX_CALL_DURATION_MINUTES': int(obj['MAX_CALL_DURATION_MINUTES']),
			'ETL_PRIORITY': int(obj['ETL_PRIORITY']),
			'SOURCE_TYPE': obj['SOURCE_TYPE']
		}

		files.append(create_preference_file(
			f"C8_{sp_name}.json",
			json.dumps(config, indent=4)
		))

	return files
