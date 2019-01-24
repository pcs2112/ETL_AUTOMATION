import ntpath
import src.preference_file_utils
import src.code_gen
import src.utils


def bulk_create_json_preference_files(in_filename):
	filename = src.preference_file_utils.get_configuration_file_path(in_filename)

	json_files = src.preference_file_utils.create_json_preference_files(filename)
	src.utils.print_green('The following JSON preference files were created:')
	for json_file in json_files:
		src.utils.print_yellow(json_file)

	data = src.preference_file_utils.get_excel_preference_file_data(filename)
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

	print("")
	src.utils.print_green('The following indices file was created:')
	src.utils.print_yellow(src.code_gen.create_column_indices(index_sql_files))
	print("")
	src.utils.print_green('Run the following commands to generate the DDL files:')
	for json_file in json_files:
		src.utils.print_yellow(f"python app.py create_sp {ntpath.basename(json_file)}")

	print("")
