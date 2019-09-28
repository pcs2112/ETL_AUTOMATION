import src.excel_utils
import src.db_utils
from src.mssql_connection import init_db, close


def get_excel_preference_file_data(file_name):
    """ Returns the preference file data in an array. """
    wb = src.excel_utils.read_workbook(file_name)
    return src.excel_utils.read_workbook_data(wb)


def is_data_type_eq(src_column, tgt_column):
    is_eq = False if src_column['data_type'] != tgt_column['data_type'] else True
    return is_eq


def is_pk_eq(src_pks, tgt_pks):
    tgt_pks_dict = {}
    for i, col_name in tgt_pks:
        tgt_pks_dict[col_name.upper()] = i

    is_eq = True
    for i, col_name in src_pks:
        if col_name not in tgt_pks_dict:
            is_eq = False
            break
            
    return is_eq


def pk_to_str(pks):
    return ', '.join(col['column_name'] for col in pks)
    

def compare_tables(config):
    # Init DB connection in the source DB
    init_db({
        'DB_SERVER': config['SOURCE_SERVER'],
        'DB_NAME': config['SOURCE_DATABASE'],
        'DB_USER': config['SOURCE_USER'],
        'DB_PASSWORD': config['SOURCE_PASSWORD'],
        'DB_DRIVER': config['SOURCE_DRIVER'],
        'DB_TRUSTED_CONNECTION': config['SOURCE_TRUSTED_CONNECTION']
    })

    # Get the table definition from the specified config
    src_tbl_definition = src.db_utils.get_table_definition(
        config['SOURCE_DATABASE'],
        config['SOURCE_SCHEMA'],
        config['SOURCE_TABLE'],
        config['SOURCE_SERVER']
    )
    
    src_tbl_pks = src.db_utils.get_primary_keys(config['SOURCE_SCHEMA'], config['SOURCE_TABLE'])
    src_row_ct = src.db_utils.get_table_record_count(config['SOURCE_SCHEMA'], config['SOURCE_TABLE'])

    close()
    
    # Init DB connection in the target DB
    init_db({
        'DB_SERVER': config['TARGET_SERVER'],
        'DB_NAME': config['TARGET_DATABASE'],
        'DB_USER': config['TARGET_USER'],
        'DB_PASSWORD': config['TARGET_PASSWORD'],
        'DB_DRIVER': config['TARGET_DRIVER'],
        'DB_TRUSTED_CONNECTION': config['TARGET_TRUSTED_CONNECTION']
    })
    
    tgt_tbl_definition = src.db_utils.get_table_definition(
        config['TARGET_DATABASE'],
        config['TARGET_SCHEMA'],
        config['TARGET_TABLE'],
        config['TARGET_SERVER']
    )

    tgt_tbl_pks = src.db_utils.get_primary_keys(config['TARGET_SCHEMA'], config['TARGET_TABLE'])
    tgt_row_ct = src.db_utils.get_table_record_count(config['TARGET_SCHEMA'], config['TARGET_TABLE'])
    
    diffs = []
    
    col_diffs = []
    for col_name, col in src_tbl_definition.items():
        if not is_data_type_eq(col, tgt_tbl_definition[col_name]):
            col_diffs.append([col_name, f"Data types are different [src={col['data_type']} tgt={tgt_tbl_definition[col_name]['data_type']}]"])
            
    if len(col_diffs) > 0:
        diffs.append(['COLUMN_NAME', 'ERRORS'])
        diffs += col_diffs
        diffs.append(['', ''])

    tbl_diffs = []
    if not is_pk_eq(src_tbl_pks, tgt_tbl_pks):
        tbl_diffs.append([f"PKs are different [src_pk=({pk_to_str(src_tbl_pks)}) tgt_pk=({pk_to_str(tgt_tbl_pks)})]"])
        
    if src_row_ct['count'] < 1:
        tbl_diffs.append([f"O rows in {config['SOURCE_SCHEMA']}.{config['SOURCE_TABLE']}."])
        
    if tgt_row_ct['count'] < 1:
        tbl_diffs.append([f"O rows in {config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}."])
        
    if len(tbl_diffs) > 0:
        diffs.append(['TABLE ERRORS'])
        diffs += tbl_diffs
        diffs.append([''])

    close()
    
    return diffs
