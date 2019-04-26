from src.config import get_config
from src.mssql_connection import fetch_row, fetch_rows
from src.utils import split_string


def get_table_exists(schema_name, table_name):
    """ Checks a table exits. """
    sql = ("SELECT * FROM sys.objects so JOIN sys.schemas ss on (so.schema_id = ss.schema_id) "
           "WHERE so.type = 'U' AND so.name = ? and ss.name = ?")

    row = fetch_row(sql, [table_name, schema_name])

    return True if row else False


def get_table_definition(db_name, schema_name, table_name, server_name, data_partition_column_name='', excluded_columns=()):
    """
    Returns the table definition.
    :param str db_name: Database name
    :param str schema_name: Schema name
    :param str table_name: Database table name
    :param str server_name: Server name
    :param str data_partition_column_name: Data partition column name
    :param list excluded_columns: Columns to exclude
    :return list
    """
    server_name = '' if server_name == '127.0.0.1' or server_name == 'localhost' else server_name
    server_name = f'[{server_name}].' if server_name else ''

    sql = ("SELECT T.name AS TABLE_NAME, C.name AS COLUMN_NAME, P.name AS DATA_TYPE, "
           "P.max_length AS SIZE, CAST(P.precision AS VARCHAR) + '/' + CAST(P.scale AS VARCHAR) AS PRECISION_SCALE, "
           "c.* FROM {0}[{1}].sys.objects AS T JOIN {0}[{1}].sys.columns AS C ON T.object_id = C.object_id "
           "JOIN {0}[{1}].sys.types AS P ON C.system_type_id = P.system_type_id "
           "JOIN sys.schemas ss ON (T.schema_id = ss.schema_id) "
           " WHERE  T.type_desc = 'USER_TABLE' and ss.name = ? "
           "and T.name = ? and P.name != 'timestamp' and P.name != 'sysname' order by column_id asc").format(server_name, db_name)

    columns = fetch_rows(sql, [schema_name, table_name])

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

        # Update the data type for the data partition column
        if data_partition_column_name != '' and column['column_name'].upper() == data_partition_column_name.upper():
            column['data_type'] = 'datetime'

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


def get_record_counts(schema_name, table_name, column_name=''):
    """
    Returns the records counts for the specified table.
    :param str schema_name: Schema name
    :param str table_name: Database table name
    :param str column_name: Search column name
    :return list
    """
    if column_name == '':
        sql = """
            SELECT
                count(*) AS 'COUNT',
                null AS 'MIN_VALUE',
                null AS 'MAX_VALUE',
                null AS 'MONTH_CNT'
            FROM
                {0}.{1} with(nolock);
            """
    else:
        sql = """
            SELECT
                count(*) AS 'COUNT',
                min({2}) AS 'MIN_VALUE',
                max({2}) AS 'MAX_VALUE',
                datediff(MONTH, MIN({2}), MAX({2})) AS 'MONTH_CNT'
            FROM
                {0}.{1} WITH(nolock);
        """

    return fetch_row(sql.format(schema_name, table_name, column_name))


def get_table_record_count(schema_name, table_name):
    """
    Returns the records count for the specified table.
    :param str schema_name: Schema name
    :param str table_name: Database table name
    :return list
    """
    sql = "select count(*) AS 'COUNT' FROM {0}.{1} with(nolock);"
    return fetch_row(sql.format(schema_name, table_name))


def get_primary_keys(schema_name, table_name):
    """ Returns the primary key columns for the specified table. """
    sql = """
    SELECT
         DISTINCT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    AND kcu.TABLE_SCHEMA = '{0}'
    AND kcu.TABLE_NAME = '{1}'
    ORDER BY kcu.ORDINAL_POSITION ASC
    """

    results = fetch_rows(sql.format(schema_name, table_name))
    columns = []
    for row in results:
        columns.append({
            'column_name': row['column_name'],
            'column_sort_order': 'ASC'
        })

    return columns


def get_primary_keys_from_str(value):
    """ Returns a list of primary keys from a string. """
    columns = []

    if value == '':
        return columns

    primary_key_strs = split_string(value, '|')
    for primary_key_str in primary_key_strs:
        primary_key_str = primary_key_str
        primary_key_parts = primary_key_str.split(' ')
        column_name = primary_key_parts[0].strip()
        column_sort_order = primary_key_parts[len(primary_key_parts) - 1].strip()
        if column_sort_order.lower() != 'desc':
            column_sort_order = 'asc'

        columns.append({
            'column_name': column_name,
            'column_sort_order': column_sort_order.upper()
        })

    return columns


def get_primary_keys_str(columns):
    """ Returns the string of primary keys. """
    out = ''
    for column in columns:
        out += f"{column['column_name']} {column['column_sort_order']}|"

    return out.rstrip('|')
