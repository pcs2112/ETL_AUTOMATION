import os
import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_column_index.sql'
out_file_name_postfix = 'INDEX.sql'


def create_column_index(schema_name, table_name, column_name, row_count, row_min_date, row_max_date, month_count):
	base_sql = src.utils.get_base_sql_code(base_sql_file_name)

	sql = base_sql.replace('<SCHEMA_NAME>', schema_name)
	sql = sql.replace('<TABLE_NAME>', table_name)
	sql = sql.replace('<COLUMN_NAME>', column_name)
	sql = sql.replace('<ROW_COUNT>', str(row_count))
	sql = sql.replace('<ROW_MIN_DATE>', str(row_min_date))
	sql = sql.replace('<ROW_MAX_DATE>', str(row_max_date))
	sql = sql.replace('<MONTH_COUNT>', str(month_count))

	# Create the file and return its path
	return src.utils.create_sql_file(
		f"C8_{schema_name}.{table_name}_{column_name}_{out_file_name_postfix}", sql
	)


def create_column_indices(files):
	contents = ''
	for file in files:
		with open(file) as fp:
			contents += fp.read() + "\n\n\n"
			
		os.remove(file)
	
	return src.utils.create_sql_file(f"C8_COLUMN_INDICES.sql", contents)
