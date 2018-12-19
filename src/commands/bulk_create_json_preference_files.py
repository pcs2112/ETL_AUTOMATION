import src.preference_file_utils
import src.code_gen


def bulk_create_json_preference_files(file_name):
	json_files = src.preference_file_utils.create_json_preference_files(
		src.preference_file_utils.get_configuration_file_path(file_name)
	)
	print('The following JSON preference files were created:')
	for json_file in json_files:
		print(json_file)
	
	data = src.preference_file_utils.get_excel_preference_file_data(
		src.preference_file_utils.get_configuration_file_path(file_name)
	)
	index_sql_files = []
	for config in data:
		index_sql_files.append(src.code_gen.create_column_index(
			config['SOURCE_SCHEMA'],
			config['SOURCE_TABLE'],
			config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'],
			config['ROW_COUNT'],
			config['ROW_MIN_DATE'],
			config['ROW_MAX_DATE'],
			config['MONTH_COUNT']
		))
	
	print("\n\n")
	print('The following indices file was created:')
	print(src.code_gen.create_column_indices(index_sql_files))
