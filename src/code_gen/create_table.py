import src.utils
import src.db_utils
import src.code_gen.utils

base_sql_file_name = 'create_table.sql'
out_file_name_postfix = 'TABLE.sql'


def create_table(config, table_definition):
    # Get the base sql for creating a table
    base_sql = src.utils.get_base_sql_code(base_sql_file_name)

    # Get the primary key
    primary_keys = config['SOURCE_TABLE_PRIMARY_KEY']
    if len(primary_keys) < 1:
        primary_keys = src.db_utils.get_primary_key(config['TARGET_SCHEMA'], config['TARGET_DATABASE'])

    # Set the columns
    columns = src.code_gen.utils.get_target_table_definition(table_definition)
    for column in config['TARGET_TABLE_EXTRA_COLUMNS']:
        columns.append(f"[{column['target_table_column_name']}] {column['data_type']}")

    columns_sql = ",\n".join(["\t" + str(column) for column in columns])
    sql = base_sql.replace('<TARGET_TABLE_COLUMNS>', columns_sql + ",\n")

    # Set the key columns
    key_columns = []
    for primary_key in primary_keys:
        key_columns.append(
            f"[{table_definition[primary_key['column_name'].upper()]['target_table_column_name']}] "
            f"{primary_key['column_sort_order']}"
        )

    if len(config['TARGET_TABLE_EXTRA_KEY_COLUMNS']) > 0:
        key_columns = key_columns + config['TARGET_TABLE_EXTRA_KEY_COLUMNS']

    if config['DATA_PARTITION_COLUMN'] != '':
        key_columns.append(f"[{table_definition[config['DATA_PARTITION_COLUMN'].upper()]['target_table_column_name']} DESC")

    key_columns_sql = ",\n".join(["\t\t" + str(key_column) for key_column in key_columns])
    sql = sql.replace('<TARGET_TABLE_KEY_COLUMNS>', key_columns_sql)
    
    # Set the target table primary key index key columns
    pk_index_key_columns = [
        f"[{table_definition[config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'].upper()]['target_table_column_name']}] ASC"
    ]
    
    # Add the primary keys
    data_partition_in_pk = False
    index_partition_in_pk = False
    for primary_key in primary_keys:
        pk_index_key_columns.append(f"[{table_definition[primary_key['column_name'].upper()]['target_table_column_name']}] {primary_key['column_sort_order']}")
        
        if config['DATA_PARTITION_COLUMN'] != '' and primary_key['column_name'].lower() == config['DATA_PARTITION_COLUMN'].lower():
            data_partition_in_pk = True
            
        if config['INDEX_PARTITION_COLUMN'] != '' and primary_key['column_name'].lower() == config['INDEX_PARTITION_COLUMN'].lower():
            index_partition_in_pk = True
        
    # Add the partition columns
    if config['DATA_PARTITION_COLUMN'] != '' and not data_partition_in_pk:
        pk_index_key_columns.append(
            f"{table_definition[config['DATA_PARTITION_COLUMN'].upper()]['target_table_column_name']} ASC"
        )
        
    if config['INDEX_PARTITION_COLUMN'] != '' and not index_partition_in_pk:
        pk_index_key_columns.append(
            f"{table_definition[config['INDEX_PARTITION_COLUMN'].upper()]['target_table_column_name']} ASC"
        )
        
    pk_index_key_columns_sql = ",\n".join(
        ["\t\t" + str(pk_index_key_column) for pk_index_key_column in pk_index_key_columns]
    )
    sql = sql.replace('<TARGET_TABLE_PK_INDEX_KEY_COLUMNS>', pk_index_key_columns_sql)

    # Set the target schema, target database and target table
    sql = sql.replace('<TARGET_SCHEMA>', config['TARGET_SCHEMA'])
    sql = sql.replace('<TARGET_DATABASE>', config['TARGET_DATABASE'])
    sql = sql.replace(
        '<TARGET_TABLE>', src.code_gen.utils.get_target_table_name(config['SOURCE_TABLE'], config['TARGET_TABLE'])
    )

    # Set the date
    sql = sql.replace('<DATE_CREATED>', src.utils.get_current_timestamp())

    # Set the data partition name
    if config['DATA_PARTITION_COLUMN'] == '':
        sql = sql.replace('<PARTITION_NAME>', f"[{config['DATA_PARTITION_FUNCTION']}]")
    else:
        sql = sql.replace('<PARTITION_NAME>', f"{config['DATA_PARTITION_FUNCTION']}({table_definition[config['DATA_PARTITION_COLUMN'].upper()]['target_table_column_name']})")

    # Set the index partition name
    if config['INDEX_PARTITION_COLUMN'] == '':
        sql = sql.replace('<INDEX_PARTITION_NAME>', f"[{config['INDEX_PARTITION_FUNCTION']}]")
    else:
        sql = sql.replace(
            '<INDEX_PARTITION_NAME>', f"{config['INDEX_PARTITION_FUNCTION']}({table_definition[config['INDEX_PARTITION_COLUMN'].upper()]['target_table_column_name']})"
        )

    # Create the file and return its path
    return src.utils.create_sql_file(
        f"C8_{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}_{out_file_name_postfix}", sql
    )
