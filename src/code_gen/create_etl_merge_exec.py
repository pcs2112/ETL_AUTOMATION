import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_etl_merge_exec.sql'


def create_etl_merge_exec(data):
    base_sql = src.utils.get_base_sql_code(base_sql_file_name)
    out_sql = ''
    
    for config in data:
        sql = base_sql
        target_table = src.code_gen.utils.get_target_table_name(config['SOURCE_TABLE'], config['TARGET_TABLE'])
        sql = sql.replace(
            '<STORED_PROCEDURE_SCHEMA>', src.code_gen.utils.get_sp_name(target_table, config['STORED_PROCEDURE_SCHEMA'])
        )
        sql = sql.replace(
            '<STORED_PROCEDURE_NAME>', src.code_gen.utils.get_sp_name(target_table, config['STORED_PROCEDURE_NAME'])
        )
        
        sql = sql.replace('<SOURCE_DATA_MART>', config['SOURCE_DATA_MART'])
        sql = sql.replace('<MIN_CALL_DURATION_MINUTES>', str(config['MIN_CALL_DURATION_MINUTES']))
        sql = sql.replace('<MAX_CALL_DURATION_MINUTES>', str(config['MAX_CALL_DURATION_MINUTES']))
        sql = sql.replace('<ETL_PRIORITY>', str(config['ETL_PRIORITY']))
        sql = sql.replace('<SOURCE_TYPE>', str(config['SOURCE_TYPE']))
        
        out_sql += sql
    
    return src.utils.create_sql_file(
        f"ETL_AUTOMATION_ACTIVE_MERGE_{src.utils.get_filename_date_postfix()}.sql",
        out_sql
    )
