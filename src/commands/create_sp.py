import src.preference_file_utils
import src.db_utils
import src.code_gen.utils
import src.utils
from src.mssql_connection import init_db, close
from src.exceptions import SearchColumnNoIndex


def create_sp(preference_filename=''):
	if preference_filename == '':
		src.utils.print_red('Please specify a valid preference file name.')
		return

	try:
		pref_config = src.preference_file_utils.get_configuration_from_preference_file(preference_filename)
	except FileExistsError as e:
		src.utils.print_red(str(e))
		return

	# Init DB connection for the source DB
	db_config = {
		'DB_SERVER': pref_config['SOURCE_SERVER'],
		'DB_NAME': pref_config['SOURCE_DATABASE'],
		'DB_USER': pref_config['SOURCE_USER'],
		'DB_PASSWORD': pref_config['SOURCE_PASSWORD'],
		'DB_DRIVER': pref_config['SOURCE_DRIVER'],
		'DB_TRUSTED_CONNECTION': pref_config['SOURCE_TRUSTED_CONNECTION']
	}

	init_db(db_config)

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
		src.utils.print_red(f"Error on {preference_filename}:")
		src.utils.print_red(str(e))
		close()
		return

	# Get table counts
	table_counts = src.db_utils.get_record_counts(
		pref_config['SOURCE_SCHEMA'],
		pref_config['SOURCE_TABLE'],
		pref_config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']
	)

	create_table_filename = src.code_gen.create_table(pref_config, table_definition)
	create_sp_filename = src.code_gen.create_sp(pref_config, table_definition, table_counts)
	src.utils.print_green('Please locate your DDL files at:')
	src.utils.print_yellow(f"Create table DDL -> {create_table_filename}")
	src.utils.print_yellow(f"Create stored procedure DDL -> {create_sp_filename}")
	print("")
	close()
