from utils import get_table_definition_from_source, get_base_sql_code, create_sql_file
from .utils import get_table_definition, get_identity_column, get_column_names, get_current_timestamp

base_sql_file_name = 'create_sp.sql'
out_file_name_postfix = 'CREATE_SP.sql'


def get_insert_columns(table_definition, extra_columns):
	insert_columns = []
	for column in table_definition:
		insert_columns.append(f"{column['column_name']}")

	for column in extra_columns:
		insert_columns.append(f"{column['column_name']}")

	return insert_columns


def get_update_values(table_definition, extra_columns):
	update_values = []
	for column in table_definition:
		update_values.append(f"{column['column_name']} = source.{column['column_name']}")

	for column in extra_columns:
		update_values.append(f"{column['column_name']} = {column['value']}")

	return update_values


def get_insert_values(table_definition, extra_columns):
	insert_values = []
	for column in table_definition:
		insert_values.append(f"source.{column['column_name']}")

	for column in extra_columns:
		insert_values.append(f"{column['value']}")

	return insert_values


def get_is_utc(table_definition, search_column):
	if search_column['is_utc']:
		return True

	for column in table_definition:
		if column['column_name'] == search_column['column_name'] and 'utc' in column['column_name'].lower():
			return True

	return False


def get_update_match_check_columns_sql(table_definition, match_check_columns):
	conditions = []
	for column in table_definition:
		if column['column_name'] in match_check_columns:
			if column['is_nullable'] == 1:
				conditions.append(
					f"(target.{column['column_name']} is NULL and source.{column['column_name']} is NOT NULL)")
				conditions.append(
					f"(target.{column['column_name']} is NOT NULL and source.{column['column_name']} is NULL)")

			conditions.append(f"target.{column['column_name']} != source.{column['column_name']}")

	return " or \n".join([str(condition) for condition in conditions])


def create_sp(config):
	# Get the table definition from the specified config
	table_definition = get_table_definition_from_source(
		config['SOURCE_DATABASE'],
		config['SOURCE_TABLE'],
		'' if config['SOURCE_SERVER'] == 'localhost' else config['SOURCE_SERVER']
	)

	period_start_prefix = ''

	# Get the base sql for creating a table
	sql = get_base_sql_code(base_sql_file_name)

	# Set UTC sections
	if get_is_utc(table_definition, config['SOURCE_TABLE_SEARCH_COLUMN']):
		sql = sql.replace('<UTC_SECTION>', get_base_sql_code('create_sp_utc_section.sql'))
		period_start_prefix = 'S'
	else:
		sql = sql.replace('<UTC_SECTION>', '')

	# Set placeholder values
	sql = sql.replace('<PERIOD_START_PREFIX>', period_start_prefix)
	sql = sql.replace('<PERIOD_END_PREFIX>', period_start_prefix)
	sql = sql.replace('<STORED_PROCEDURE_NAME>', config['STORED_PROCEDURE_NAME'])
	sql = sql.replace('<SOURCE_SERVER>', config['SOURCE_SERVER'])
	sql = sql.replace('<SOURCE_DATABASE>', config['SOURCE_DATABASE'])
	sql = sql.replace('<SOURCE_SCHEMA>', config['SOURCE_SCHEMA'])
	sql = sql.replace('<SOURCE_TABLE>', config['SOURCE_TABLE'])
	sql = sql.replace('<SOURCE_DATA_MART>', config['SOURCE_DATA_MART'])
	sql = sql.replace('<SOURCE_TABLE_SEARCH_COLUMN>', config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'])
	sql = sql.replace('<TARGET_SERVER>', config['TARGET_SERVER'])
	sql = sql.replace('<TARGET_DATABASE>', config['TARGET_DATABASE'])
	sql = sql.replace('<TARGET_SCHEMA>', config['TARGET_SCHEMA'])
	sql = sql.replace('<TARGET_TABLE>', config['TARGET_TABLE'])
	sql = sql.replace('<MIN_CALL_DURATION_MINUTES>', str(config['MIN_CALL_DURATION_MINUTES']))
	sql = sql.replace('<MAX_CALL_DURATION_MINUTES>', str(config['MAX_CALL_DURATION_MINUTES']))
	sql = sql.replace('<ETL_PRIORITY>', str(config['ETL_PRIORITY']))
	sql = sql.replace('<SOURCE_TYPE>', str(config['SOURCE_TYPE']))
	sql = sql.replace('<DATE_CREATED>', get_current_timestamp())

	# Set the source table definition columns
	source_table_column_definition = get_table_definition(table_definition)
	source_table_column_definition_sql = ",\n".join(["\t\t" + str(column) for column in source_table_column_definition])
	sql = sql.replace('<SOURCE_TABLE_COLUMN_DEFINITION>', source_table_column_definition_sql)

	# Set the source table column names
	source_table_column_names = get_column_names(table_definition)
	source_table_column_names_sql = ",\n".join(["\t\t" + str(column) for column in source_table_column_names])
	sql = sql.replace('<SOURCE_TABLE_COLUMN_NAMES>', source_table_column_names_sql)

	# Set the identity column
	if config['SOURCE_TABLE_PRIMARY_KEY'] != '':
		sql = sql.replace('<SOURCE_TABLE_PRIMARY_KEY>', config['SOURCE_TABLE_PRIMARY_KEY'])
	else:
		identity_column = get_identity_column(table_definition)
		sql = sql.replace('<SOURCE_TABLE_PRIMARY_KEY>', identity_column['column_name'])

	# Set the target table update columns and values
	target_table_update_values = get_update_values(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
	target_table_update_values_sql = ",\n".join(["\t\t" + str(column) for column in target_table_update_values])
	sql = sql.replace('<TARGET_TABLE_UPDATE_VALUES>', target_table_update_values_sql)

	# Set the target table insert columns and values
	target_table_insert_columns = get_insert_columns(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
	target_table_insert_columns_sql = ",\n".join(["\t\t" + str(column) for column in target_table_insert_columns])
	sql = sql.replace('<TARGET_TABLE_INSERT_COLUMNS>', target_table_insert_columns_sql)

	target_table_insert_values = get_insert_values(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
	target_table_insert_values_sql = ",\n".join(["\t\t" + str(column) for column in target_table_insert_values])
	sql = sql.replace('<TARGET_TABLE_INSERT_VALUES>', target_table_insert_values_sql)

	# Set the target table search condition
	sql = sql.replace('<SOURCE_TABLE_SEARCH_CONDITION>', config['SOURCE_TABLE_SEARCH_CONDITION'])

	# Set the match check columns
	if len(config['UPDATE_MATCH_CHECK_COLUMNS']) > 0:
		sql = sql.replace('<MATCH_SECTION>', get_base_sql_code('create_sp_match_section.sql'))
		sql = sql.replace('<UPDATE_MATCH_CHECK_COLUMNS>', get_update_match_check_columns_sql(
			table_definition, config['UPDATE_MATCH_CHECK_COLUMNS']
		))
	else:
		sql = sql.replace('<MATCH_SECTION>', get_base_sql_code('create_sp_match_section_empty.sql'))

	# Create the file and return its path
	return create_sql_file(f"{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}_{out_file_name_postfix}", sql)
