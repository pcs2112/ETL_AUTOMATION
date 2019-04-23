import src.db_utils
import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_table.sql'
out_file_name_postfix = 'TABLE.sql'


def get_target_table_column_name(column_name, table_definition):
    norm_column_name = column_name.upper()
    if norm_column_name not in table_definition:
        return column_name
    
    return table_definition[norm_column_name]['target_table_column_name']
    

def create_table(config, table_definition):
    # Get the base sql for creating a table
    base_sql = src.utils.get_base_sql_code(base_sql_file_name)

    # Get the primary keys
    primary_keys = config['SOURCE_TABLE_PRIMARY_KEY']
    if len(primary_keys) < 1:
        primary_keys = src.db_utils.get_primary_keys(config['TARGET_SCHEMA'], config['TARGET_DATABASE'])

    # Set the target table columns
    target_tbl_cols = src.code_gen.utils.get_target_table_definition(table_definition)
    for col in config['TARGET_TABLE_EXTRA_COLUMNS']:
        target_tbl_cols.append(
            f"[{get_target_table_column_name(col['column_name'], table_definition)} {col['data_type']}"
        )

    target_table_cols_sql = ",\n".join(["\t" + str(col) for col in target_tbl_cols])
    sql = base_sql.replace('<TARGET_TABLE_COLUMNS>', target_table_cols_sql + ",\n")

    # Set the target table key columns
    target_tbl_key_cols = []
    for primary_key in primary_keys:
        target_tbl_key_cols.append(
            f"[{get_target_table_column_name(primary_key['column_name'], table_definition)}] "
            f"{primary_key['column_sort_order']}"
        )

    if len(config['TARGET_TABLE_EXTRA_KEY_COLUMNS']) > 0:
        for extra_key_column in config['TARGET_TABLE_EXTRA_KEY_COLUMNS']:
            target_tbl_key_cols.append(get_target_table_column_name(extra_key_column, table_definition))

    if config['DATA_PARTITION_COLUMN'] != '':
        target_tbl_key_cols.append(
            f"[{get_target_table_column_name(config['DATA_PARTITION_COLUMN'], table_definition)} DESC"
        )

    target_tbl_key_cols_sql = ",\n".join(["\t\t" + str(key_col) for key_col in target_tbl_key_cols])
    sql = sql.replace('<TARGET_TABLE_KEY_COLUMNS>', target_tbl_key_cols_sql)
    
    # Set the target table primary key index key columns
    target_tbl_pk_index_key_cols = [
        f"[{get_target_table_column_name(config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'], table_definition)}] ASC"
    ]
    
    # Add the target table primary keys
    data_partition_in_pk = False
    index_partition_in_pk = False
    for primary_key in primary_keys:
        target_tbl_pk_index_key_cols.append(
            f"[{get_target_table_column_name(primary_key['column_name'], table_definition)}] "
            f"{primary_key['column_sort_order']}"
        )
        
        if config['DATA_PARTITION_COLUMN'] != '' \
                and primary_key['column_name'].lower() == config['DATA_PARTITION_COLUMN'].lower():
            data_partition_in_pk = True
            
        if config['INDEX_PARTITION_COLUMN'] != '' \
                and primary_key['column_name'].lower() == config['INDEX_PARTITION_COLUMN'].lower():
            index_partition_in_pk = True
        
    # Add the partition columns to the target table pk key columns
    if config['DATA_PARTITION_COLUMN'] != '' and not data_partition_in_pk:
        target_tbl_pk_index_key_cols.append(
            f"{get_target_table_column_name(config['DATA_PARTITION_COLUMN'], table_definition)} ASC"
        )
        
    if config['INDEX_PARTITION_COLUMN'] != '' and not index_partition_in_pk:
        target_tbl_pk_index_key_cols.append(
            f"{get_target_table_column_name(config['INDEX_PARTITION_COLUMN'], table_definition)} ASC"
        )
        
    target_tbl_pk_index_key_cols_sql = ",\n".join(
        ["\t\t" + str(pk_index_key_col) for pk_index_key_col in target_tbl_pk_index_key_cols]
    )
    sql = sql.replace('<TARGET_TABLE_PK_INDEX_KEY_COLUMNS>', target_tbl_pk_index_key_cols_sql)

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
        sql = sql.replace(
            '<PARTITION_NAME>',
            f"{config['DATA_PARTITION_FUNCTION']}"
            f"({get_target_table_column_name(config['DATA_PARTITION_COLUMN'], table_definition)})"
        )

    # Set the index partition name
    if config['INDEX_PARTITION_COLUMN'] == '':
        sql = sql.replace('<INDEX_PARTITION_NAME>', f"[{config['INDEX_PARTITION_FUNCTION']}]")
    else:
        sql = sql.replace(
            '<INDEX_PARTITION_NAME>',
            f"{config['INDEX_PARTITION_FUNCTION']}"
            f"({get_target_table_column_name(config['INDEX_PARTITION_COLUMN'], table_definition)})"
        )

    # Create the file and return its path
    return src.utils.create_sql_file(
        f"C8_{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}_{out_file_name_postfix}", sql
    )
