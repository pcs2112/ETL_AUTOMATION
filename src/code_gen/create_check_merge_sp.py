import src.utils
import src.code_gen.utils
from datetime import *

base_sql_file_name = 'create_merge_check_sp.sql'
out_file_name_postfix = 'SP.sql'
row_min_date = datetime.strptime('2000-01-01', '%Y-%m-%d')


def get_is_utc(table_definition, search_column):
    if search_column['is_utc']:
        return True
    
    for key, column in table_definition.items():
        if column['column_name'] == search_column['column_name'] and 'utc' in column['column_name'].lower():
            return True
    
    return False


def create_check_merge_sp(config, table_definition, table_counts=None):
    # Get the base sql for creating a table
    sql = src.utils.get_base_sql_code(base_sql_file_name)
    
    # Get is UTC
    is_utc = get_is_utc(table_definition, config['SOURCE_TABLE_SEARCH_COLUMN'])
    
    # Set placeholder values
    target_table = src.code_gen.utils.get_target_table_name(config['SOURCE_TABLE'], config['TARGET_TABLE'])
    sql = sql.replace(
        '<STORED_PROCEDURE_SCHEMA>', src.code_gen.utils.get_sp_name(target_table, config['STORED_PROCEDURE_SCHEMA'])
    )
    sql = sql.replace(
        '<STORED_PROCEDURE_NAME>',
        src.code_gen.utils.get_sp_name(target_table, 'CHECK_' + config['STORED_PROCEDURE_NAME'])
    )
    sql = sql.replace('<SOURCE_SERVER>', config['SOURCE_SERVER'])
    if config['SOURCE_SERVER'] != 'localhost' and config['SOURCE_SERVER'] != '127.0.0.1':
        sql = sql.replace('<SOURCE_SERVER_SELECT>', f"[{config['SOURCE_SERVER']}].")
    else:
        sql = sql.replace('<SOURCE_SERVER_SELECT>', '')
    
    sql = sql.replace('<SOURCE_DATABASE>', config['SOURCE_DATABASE'])
    sql = sql.replace('<SOURCE_SCHEMA>', config['SOURCE_SCHEMA'])
    sql = sql.replace('<SOURCE_TABLE>', config['SOURCE_TABLE'])
    sql = sql.replace('<SOURCE_DATA_MART>', config['SOURCE_DATA_MART'])
    sql = sql.replace('<SOURCE_TABLE_SEARCH_COLUMN>', config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'])
    sql = sql.replace('<TARGET_SERVER>', config['TARGET_SERVER'])
    sql = sql.replace('<TARGET_DATABASE>', config['TARGET_DATABASE'])
    sql = sql.replace('<TARGET_SCHEMA>', config['TARGET_SCHEMA'])
    sql = sql.replace('<TARGET_TABLE>', target_table)
    sql = sql.replace('<MIN_CALL_DURATION_MINUTES>', str(config['MIN_CALL_DURATION_MINUTES']))
    sql = sql.replace('<MAX_CALL_DURATION_MINUTES>', str(config['MAX_CALL_DURATION_MINUTES']))
    sql = sql.replace('<ETL_PRIORITY>', str(config['ETL_PRIORITY']))
    sql = sql.replace('<SOURCE_TYPE>', str(config['SOURCE_TYPE']))
    sql = sql.replace('<DATE_CREATED>', src.utils.get_current_timestamp())
    sql = sql.replace('<IS_UTC>', str(1 if is_utc else 0))
    
    if table_counts is None:
        table_counts['min_value'] = row_min_date
    
    period_start_date = src.utils.get_default_value(table_counts['min_value'], row_min_date)
    sql = sql.replace('<PERIOD_START_DATE>', period_start_date.strftime('%Y-%m-%d'))
    
    # Set the identity column
    if config['SOURCE_TABLE_PRIMARY_KEY'] != '':
        sql = sql.replace('<TARGET_TABLE_PRIMARY_KEY>', config['SOURCE_TABLE_PRIMARY_KEY'])
    else:
        identity_column = src.code_gen.utils.get_identity_column(table_definition)
        sql = sql.replace('<TARGET_TABLE_PRIMARY_KEY>', identity_column['target_table_column_name'])
    
    # Create the file and return its path C8_<STORED_PROCEDURE_SCHEMA>.CHECK_<STORED_PROCEDURE_NAME>
    return src.utils.create_sql_file(
        f"C8_{config['STORED_PROCEDURE_SCHEMA']}.CHECK_{config['STORED_PROCEDURE_NAME']}_{out_file_name_postfix}", sql
    )
