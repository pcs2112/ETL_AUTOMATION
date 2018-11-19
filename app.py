import sys
import code_generation
from config import get_config
from mssql_connection import init_db, close
from mssql_functions import get_configuration_from_preference_file


def create_stored_procedure(preference_filename=''):
    if preference_filename == '':
        print('Please specify a valid preference file name.')
        return

    try:
        pref_config = get_configuration_from_preference_file(preference_filename)
    except FileExistsError as e:
        print(str(e))
        return

    create_table_filename = code_generation.create_table(pref_config)
    create_stored_procedure_filename = code_generation.create_stored_procedure(pref_config)
    print('Please locate your DDL files at:')
    print(f"create table DDL -> {create_table_filename}")
    print(f"create stored procedure DDL -> {create_stored_procedure_filename}")


arg_count = len(sys.argv)
if arg_count == 1:
    print("Please enter a command.")
    exit()

try:
    cmd = locals()[sys.argv[1]]
except KeyError as e:
    print(f"\"{sys.argv[1]}\" is an invalid command.")
    exit()

# Init DB
init_db(get_config())

if len(sys.argv) > 2:
    cmd(*sys.argv[2:len(sys.argv)])
else:
    cmd()

# Close DB
close()
