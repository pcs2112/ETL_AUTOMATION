import sys
import code_generation
from config import get_config
from mssql_connection import init_db, close
from utils import (
	get_configuration_from_preference_file, get_table_definition_from_source, validate_preference_file,
	create_preference_files
)


def create_stored_procedure(preference_filename=''):
	if preference_filename == '':
		print('Please specify a valid preference file name.')
		return

	try:
		pref_config = get_configuration_from_preference_file(preference_filename)
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
	table_definition = get_table_definition_from_source(
		pref_config['SOURCE_DATABASE'],
		pref_config['SOURCE_TABLE'],
		'' if pref_config['SOURCE_SERVER'] == 'localhost' else pref_config['SOURCE_SERVER']
	)

	try:
		validate_preference_file(table_definition, pref_config)
	except ValueError as e:
		print(f"Error on {preference_filename}:")
		print(str(e))
		close()
		return

	create_table_filename = code_generation.create_table(pref_config, table_definition)
	create_sp_filename = code_generation.create_sp(pref_config, table_definition)
	print('Please locate your DDL files at:')
	print(f"Create table DDL -> {create_table_filename}")
	print(f"Create stored procedure DDL -> {create_sp_filename}")
	close()


def bulk_create_preference_files(file_name):
	files = create_preference_files(file_name)
	print('The following files were created:')
	for file in files:
		print(file)


arg_count = len(sys.argv)
if arg_count == 1:
	print("Please enter a command.")
	exit()

try:
	cmd = locals()[sys.argv[1]]
except KeyError as e:
	print(f"\"{sys.argv[1]}\" is an invalid command.")
	exit()

if len(sys.argv) > 2:
	cmd(*sys.argv[2:len(sys.argv)])
else:
	cmd()
