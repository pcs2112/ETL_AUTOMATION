import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_table.sql'
out_file_name_postfix = 'TABLE.sql'


def create_table(config, table_definition):
	# Get the base sql for creating a table
	base_sql = src.utils.get_base_sql_code(base_sql_file_name)

	# Set the columns
	columns = src.code_gen.utils.get_table_definition(table_definition)
	for column in config['TARGET_TABLE_EXTRA_COLUMNS']:
		columns.append(f"{column['column_name']} {column['data_type']}")

	columns_sql = ",\n".join(["\t" + str(column) for column in columns])
	sql = base_sql.replace('<TARGET_TABLE_COLUMNS>', columns_sql + ",\n")

	# Set the key columns
	key_columns = []
	if config['SOURCE_TABLE_PRIMARY_KEY'] != '':
		key_columns = [f"{config['SOURCE_TABLE_PRIMARY_KEY']} ASC"]
	else:
		identity_column = src.code_gen.utils.get_identity_column(table_definition)
		if identity_column:
			key_columns = [f"{identity_column['column_name']} ASC"]

	if len(config['TARGET_TABLE_EXTRA_KEY_COLUMNS']) > 0:
		key_columns = key_columns + config['TARGET_TABLE_EXTRA_KEY_COLUMNS']

	if config['DATA_PARTITION_COLUMN'] != '':
		key_columns.append(f"{config['DATA_PARTITION_COLUMN']} DESC")

	key_columns_sql = ",\n".join(["\t\t" + str(key_column) for key_column in key_columns])
	sql = sql.replace('<TARGET_TABLE_KEY_COLUMNS>', key_columns_sql)

	# Set the target schema and target table
	sql = sql.replace('<TARGET_SCHEMA>', config['TARGET_SCHEMA'])
	sql = sql.replace(
		'<TARGET_TABLE>', src.code_gen.utils.get_target_table_name(config['SOURCE_TABLE'], config['TARGET_TABLE'])
	)

	# Set the date
	sql = sql.replace('<DATE_CREATED>', src.utils.get_current_timestamp())

	# Set the data partition name
	if config['DATA_PARTITION_COLUMN'] == '':
		sql = sql.replace('<PARTITION_NAME>', f"[{config['DATA_PARTITION_FUNCTION']}]")
	else:
		sql = sql.replace('<PARTITION_NAME>', f"{config['DATA_PARTITION_FUNCTION']}({config['DATA_PARTITION_COLUMN']})")

	# Set the index partition name
	if config['INDEX_PARTITION_COLUMN'] == '':
		sql = sql.replace('<INDEX_PARTITION_NAME>', f"[{config['INDEX_PARTITION_FUNCTION']}]")
	else:
		sql = sql.replace(
			'<INDEX_PARTITION_NAME>', f"{config['INDEX_PARTITION_FUNCTION']}({config['INDEX_PARTITION_COLUMN']})"
		)

	# Create the file and return its path
	return src.utils.create_sql_file(
		f"C8_{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}_{out_file_name_postfix}", sql
	)
