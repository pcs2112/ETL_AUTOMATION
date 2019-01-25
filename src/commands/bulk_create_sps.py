import sys, traceback
import src.preference_file_utils
import src.code_gen
import src.db_utils
from src.mssql_connection import init_db, get_db, close
from src.exceptions import SearchColumnNoIndex


def bulk_create_sps(in_filename):
	filename = src.preference_file_utils.get_configuration_file_path(in_filename)

	# Get the data for the excel preference file to create the indices file
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
	print('The following indices file was created:')
	print(src.code_gen.create_column_indices(index_sql_files))
	print("")

	# Get the JSON preference files
	json_files = src.preference_file_utils.create_json_preference_files(filename)
	for json_file in json_files:
		print("")
		print(f'Executing {json_file}...')
		try:
			create_sp(json_file)
		except Exception as e:
			close()
			print('Exception:')
			traceback.print_exc(file=sys.stdout)
			pass

		print(f'finished executing {json_file}.')
		print("")


def create_sp(preference_filename):
	pref_config = src.preference_file_utils.get_configuration_from_preference_file(preference_filename)

	# Init DB connection in the source DB
	init_db({
		'DB_SERVER': pref_config['SOURCE_SERVER'],
		'DB_NAME': pref_config['SOURCE_DATABASE'],
		'DB_USER': pref_config['SOURCE_USER'],
		'DB_PASSWORD': pref_config['SOURCE_PASSWORD'],
		'DB_DRIVER': pref_config['SOURCE_DRIVER'],
		'DB_TRUSTED_CONNECTION': pref_config['SOURCE_TRUSTED_CONNECTION']
	})

	# Get the table definition from the specified config
	table_definition = src.db_utils.get_table_definition(
		pref_config['SOURCE_DATABASE'],
		pref_config['SOURCE_SCHEMA'],
		pref_config['SOURCE_TABLE'],
		pref_config['SOURCE_SERVER'],
		pref_config['SOURCE_EXCLUDED_COLUMNS']
	)

	try:
		src.preference_file_utils.validate_preference_file_config(pref_config, table_definition)
	except SearchColumnNoIndex:
		pass
	except Exception as e:
		raise e

	# Get table counts
	table_counts = src.db_utils.get_record_counts(
		pref_config['SOURCE_SCHEMA'],
		pref_config['SOURCE_TABLE'],
		pref_config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']
	)

	create_table_filename = src.code_gen.create_table(pref_config, table_definition)
	create_sp_filename = src.code_gen.create_sp(pref_config, table_definition, table_counts)

	close()

	# Init DB connection in the target DB
	init_db({
		'DB_SERVER': pref_config['TARGET_SERVER'],
		'DB_NAME': pref_config['TARGET_DATABASE'],
		'DB_USER': pref_config['TARGET_USER'],
		'DB_PASSWORD': pref_config['TARGET_PASSWORD'],
		'DB_DRIVER': pref_config['TARGET_DRIVER'],
		'DB_TRUSTED_CONNECTION': pref_config['TARGET_TRUSTED_CONNECTION']
	})

	# Create the table
	with open(create_table_filename) as fp:
		create_table_sql = fp.read()

	# Get create table sql parts
	create_table_sql_parts = create_table_sql.split('GO -- delimiter')
	if len(create_table_sql_parts) > 0:
		for i, sql in enumerate(create_table_sql_parts):
			normalized_sql = sql.strip()
			if i > 0 and normalized_sql != '':
				with get_db().cursor() as cursor:
					try:
						cursor.execute(normalized_sql)
					except Exception as e:
						print(f"Error on create table for file {create_table_filename}.")
						raise e

	# Create the SP
	with open(create_sp_filename) as fp:
		create_sp_sql = fp.read()

	create_sp_sql_parts = create_sp_sql.split('GO -- delimiter')
	if len(create_sp_sql_parts) > 0:
		for i, sql in enumerate(create_sp_sql_parts):
			normalized_sql = sql.strip()
			if i > 0 and normalized_sql != '':
				with get_db().cursor() as cursor:
					try:
						cursor.execute(normalized_sql)
					except Exception as e:
						print(f"Error on create sp for file {create_sp_filename}.")
						raise e

	close()
