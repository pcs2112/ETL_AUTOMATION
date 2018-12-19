import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_column_in.sql'
out_file_name_postfix = 'INDEX.sql'


def create_column_index(schema_name, table_name, column_name):
	base_sql = src.utils.get_base_sql_code(base_sql_file_name)

	sql = base_sql.replace('<SCHEMA_NAME>', schema_name)
	sql = sql.replace('<TABLE_NAME>', table_name)
	sql = sql.replace('<COLUMN_NAME>', column_name)

	# Create the file and return its path
	return src.utils.create_sql_file(
		f"C8_{schema_name}.{table_name}_{column_name}_{out_file_name_postfix}", sql
	)
