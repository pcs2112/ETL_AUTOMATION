from src.config import get_config
from src.mssql_connection import fetch_row, fetch_rows


def get_table_definition(db_name, table_name, server_name, excluded_columns=()):
	"""
	Returns the table definition.
	:param str db_name: Database name
	:param str table_name: Database table name
	:param str server_name: Server name
	:param list excluded_columns: Columns to exclude
	:return list
	"""
	server_name = '' if server_name == '127.0.0.1' or server_name == 'localhost' else server_name
	server_name = f'[{server_name}].' if server_name else ''

	sql = ("SELECT T.name AS TABLE_NAME, C.name AS COLUMN_NAME, P.name AS DATA_TYPE, "
			"P.max_length AS SIZE, CAST(P.precision AS VARCHAR) + '/' + CAST(P.scale AS VARCHAR) AS PRECISION_SCALE, "
			"c.* FROM {0}[{1}].sys.objects AS T JOIN {0}[{1}].sys.columns AS C ON T.object_id = C.object_id "
			"JOIN {0}[{1}].sys.types AS P ON C.system_type_id = P.system_type_id WHERE  T.type_desc = 'USER_TABLE' "
			"and T.name = ? and P.name != 'timestamp' order by column_id asc").format(server_name, db_name, table_name)

	columns = fetch_rows(sql, [table_name])

	default_columns = {
		'ID': 1,
		'INSERT_DTTM': 1,
		'UPDATE_DTTM': 1,
		'LST_MOD_USER': 1,
		'MSTR_LOAD_ID': 1,
		'ACTIVE_FLAG': 1
	}

	target_table_column_prefix = get_config()['TARGET_TABLE_COLUMN_PREFIX']
	out_columns = {}

	for column in columns:
		if column['column_name'].upper() in default_columns:
			column['target_table_column_name'] = target_table_column_prefix + column['column_name']
		else:
			column['target_table_column_name'] = column['column_name']

		out_columns[column['column_name'].upper()] = column

	if len(excluded_columns) > 0:
		for excluded_column in excluded_columns:
			out_columns.pop(excluded_column)

	return out_columns


def column_index_exists(schema_name, table_name, column_name):
	"""
	Checks an index exists for the specified column name.
	:param str schema_name: Schema name
	:param str table_name: Database table name
	:param str column_name: Search column name
	:return bool
	"""
	sql = """
with table_w_DateLstMod (
	SchemaName,
	TableName,
	ColName,
	column_id,
	DATA_TYPE,
	object_id
) as (
	SELECT distinct
		s.name,
		t.name,
		c.name,
		c.column_id,
		case 
			when p.name = 'numeric' then 'numeric(' + cast(c.precision as varchar(5)) + ',' + cast(c.scale as varchar(5)) + ')'
			else (
				case
				when p.name = 'varchar'
				then 'varchar(' + cast(c.max_length as varchar(5)) + ')' else p.name end) end as 'data_type', t.object_id
				FROM sys.schemas s with(nolock)
				JOIN sys.tables t with(nolock) ON (s.schema_id = t.schema_id)
				JOIN sys.columns c with(nolock) ON (c.object_id = t.object_id)
				JOIN sys.types P ON C.system_type_id = P.system_type_id
				WHERE s.name = ?
				and t.name = ?
				and c.name = ?
				and t.type = 'U'
			),
			table_indexes (
			SchemaName,
			TableName,
			ColName,
			DATA_TYPE,
			index_name,
			index_type,
			object_id ) as (
				select distinct
					td.SchemaName,
					td.TableName,
					td.ColName,
					td.DATA_TYPE,
					i.name,
					i.type_desc,
					td.object_id
				FROM table_w_DateLstMod td
				JOIN [sys].[index_columns] ic with(nolock)
				on (ic.object_id = td.object_id and ic.column_id = td.column_id and ic.index_column_id = 1)
				JOIN sys.indexes i with(nolock)
				ON (i.object_id = td.object_id and ic.index_id = i.index_id)
			)
select
	SchemaName, TableName, ColName, DATA_TYPE, index_name, index_type, object_id
from table_indexes;
"""
	row = fetch_row(sql, [schema_name, table_name, column_name])
	return row is not None


def get_record_counts(schema_name, table_name, column_name):
	"""
	Returns the records counts for the specified table.
	:param str schema_name: Schema name
	:param str table_name: Database table name
	:param str column_name: Search column name
	:return list
	"""
	sql = """
select
	count(*) COUNT,
	min({2}) MIN_VALUE,
	max({2}) MAX_VALUE,
	datediff(month, min({2}), max({2})) MONTH_CNT
from
	{0}.{1} with(nolock);
	"""

	return fetch_row(sql.format(schema_name, table_name, column_name))
