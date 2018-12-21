import os
import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_column_index.sql'
out_file_name_postfix = 'INDEX.sql'


def create_column_index(schema_name, table_name, column_name, row_count, row_min_date, row_max_date, month_count):
	"""
	Creates the DDL for creating an index on a column.
	:param str schema_name: Database name
	:param str table_name: Database table name
	:param str column_name: Column used to add the index
	:param int row_count: Row count for the table
	:param str row_min_date: Min date for the table
	:param str row_max_date: Max date for the table
	:param int month_count: Number of months between the min and max dates
	:return str: Returns the file name
	"""
	base_sql = src.utils.get_base_sql_code(base_sql_file_name)

	sql = base_sql.replace('<SCHEMA_NAME>', schema_name)
	sql = sql.replace('<TABLE_NAME>', table_name)
	sql = sql.replace('<COLUMN_NAME>', column_name)
	sql = sql.replace('<ROW_COUNT>', str(row_count))
	sql = sql.replace('<ROW_MIN_DATE>', str(row_min_date))
	sql = sql.replace('<ROW_MAX_DATE>', str(row_max_date))
	sql = sql.replace('<MONTH_COUNT>', str(month_count))

	return src.utils.create_sql_file(
		f"C8_{schema_name}.{table_name}_{column_name}_{out_file_name_postfix}_{src.utils.get_filename_date_postfix()}",
		sql
	)


def create_column_indices(sql_files):
	""" Creates a DDL file using the contents from the specified DDL files. """
	contents = ''
	for sql_file in sql_files:
		with open(sql_file) as fp:
			contents += fp.read() + "\n\n\n"
			
	for sql_file in sql_files:
		if os.path.exists(sql_file):
			os.remove(sql_file)
	
	return src.utils.create_sql_file(f"C8_COLUMN_INDICES_{src.utils.get_filename_date_postfix()}.sql", contents)
