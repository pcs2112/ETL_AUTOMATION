import src.preference_file_utils
import src.db_utils
import src.code_gen.utils
import src.utils
from src.config import get_config
from src.mssql_connection import init_db, close
from src.exceptions import SearchColumnNoIndex


def create_sp(preference_filename=''):
	if preference_filename == '':
		print('Please specify a valid preference file name.')
		return

	try:
		pref_config = src.preference_file_utils.get_configuration_from_preference_file(preference_filename)
	except FileExistsError as e:
		print(str(e))
		return

	# Init DB connection using the preference file
	if pref_config['SOURCE_SERVER'] == 'localhost':
		db_config = get_config()
	else:
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
		src.preference_file_utils.validate_preference_file_config(pref_config, table_definition)
	except SearchColumnNoIndex:
		pass
	except Exception as e:
		print(f"Error on {preference_filename}:")
		print(str(e))
		close()
		return

	create_table_filename = src.code_gen.create_table(pref_config, table_definition)
	create_sp_filename = src.code_gen.create_sp(pref_config, table_definition)
	print('Please locate your DDL files at:')
	print(f"Create table DDL -> {create_table_filename}")
	print(f"Create stored procedure DDL -> {create_sp_filename}")
	print("")
	close()
