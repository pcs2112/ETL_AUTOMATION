from config import get_config
from mssql_connection import init_db, close
from mssql_functions import get_configuration_from_preference_file
from code_generation.create_table import create_table
from code_generation.create_stored_procedure import create_stored_procedure

# Init DB
init_db(get_config())

pref_config = get_configuration_from_preference_file('ETL_MWH.ERROR_RESOLUTIONS.json')
print(create_table(pref_config))
print(create_stored_procedure(pref_config))

# Close DB
close()
