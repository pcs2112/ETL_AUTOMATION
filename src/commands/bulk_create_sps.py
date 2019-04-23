import sys, traceback
import src.preference_file_utils
import src.code_gen
import src.code_gen.utils
import src.db_utils
from src.mssql_connection import init_db, get_db, close
from src.exceptions import SearchColumnNoIndex


def bulk_create_sps(in_filename):
    filename = src.preference_file_utils.get_configuration_file_path(in_filename)
    
    # Get the data for the excel preference file to create the indices file
    data = src.preference_file_utils.get_excel_preference_file_data(filename)
    index_sql_files = []
    for config in data:
        index_sql_files.append(src.code_gen.create_column_index(
            config['SOURCE_SCHEMA'],
            config['SOURCE_TABLE'],
            config['SOURCE_TABLE_SEARCH_COLUMN']['column_name'],
            config['ROW_COUNT'],
            config['ROW_MIN_DATE'],
            config['ROW_MAX_DATE'],
            config['MONTH_COUNT']
        ))
    
    print("")
    print('The following indices file was created:')
    print(src.code_gen.create_column_indices(index_sql_files))
    print("")
    
    # Generate ETL merge exec file
    print("")
    print('The following ETL automation merge exec file was created:')
    print(src.code_gen.create_etl_merge_exec(data))
    print("")
    
    # Get the JSON preference files
    json_files = src.preference_file_utils.create_json_preference_files(filename)
    for json_file in json_files:
        print("")
        print(f'Executing {json_file}...')
        try:
            create_sp(json_file)
        except Exception as e:
            close()
            print('Exception:')
            traceback.print_exc(file=sys.stdout)
            pass
        
        print(f'finished executing {json_file}.')
        print("")


def create_sp(preference_filename):
    pref_config = src.preference_file_utils.get_configuration_from_preference_file(preference_filename)
    
    # Init DB connection in the source DB
    init_db({
        'DB_SERVER': pref_config['SOURCE_SERVER'],
        'DB_NAME': pref_config['SOURCE_DATABASE'],
        'DB_USER': pref_config['SOURCE_USER'],
        'DB_PASSWORD': pref_config['SOURCE_PASSWORD'],
        'DB_DRIVER': pref_config['SOURCE_DRIVER'],
        'DB_TRUSTED_CONNECTION': pref_config['SOURCE_TRUSTED_CONNECTION']
    })
    
    # Get the table definition from the specified config
    table_definition = src.db_utils.get_table_definition(
        pref_config['SOURCE_DATABASE'],
        pref_config['SOURCE_SCHEMA'],
        pref_config['SOURCE_TABLE'],
        pref_config['SOURCE_SERVER'],
        pref_config['SOURCE_EXCLUDED_COLUMNS']
    )
    
    try:
        src.preference_file_utils.validate_preference_file_config(pref_config, table_definition)
    except SearchColumnNoIndex:
        pass
    except Exception as e:
        raise e
    
    # Get table counts
    table_counts = src.db_utils.get_record_counts(
        pref_config['SOURCE_SCHEMA'],
        pref_config['SOURCE_TABLE'],
        pref_config['SOURCE_TABLE_SEARCH_COLUMN']['column_name']
    )
    
    create_table_filename = src.code_gen.create_table(pref_config, table_definition)
    create_sp_filename = src.code_gen.create_sp(pref_config, table_definition, table_counts)
    create_check_merge_sp_filename = src.code_gen.create_check_merge_sp(pref_config, table_definition, table_counts)
    
    close()
    
    # Init DB connection in the target DB
    init_db({
        'DB_SERVER': pref_config['TARGET_SERVER'],
        'DB_NAME': pref_config['TARGET_DATABASE'],
        'DB_USER': pref_config['TARGET_USER'],
        'DB_PASSWORD': pref_config['TARGET_PASSWORD'],
        'DB_DRIVER': pref_config['TARGET_DRIVER'],
        'DB_TRUSTED_CONNECTION': pref_config['TARGET_TRUSTED_CONNECTION']
    })
    
    # Create the table
    if pref_config['TARGET_TABLE_EXISTS'] and not pref_config['TARGET_TABLE_RECREATE']:
        print(f"{pref_config['TARGET_TABLE']} table exists.")
    else:
        if pref_config['TARGET_TABLE_EXISTS']:
            with get_db().cursor() as cursor:
                table_name = pref_config['TARGET_SCHEMA'] + '.' + pref_config['TARGET_TABLE']
                try:
                    cursor.execute(f"DROP TABLE {table_name}")
                except Exception as e:
                    print(f"Error dropping table {table_name}.")
                    raise e
        
        with open(create_table_filename) as fp:
            create_table_sql = fp.read()
        
        # Get create table sql parts
        create_table_sql_parts = create_table_sql.split('GO -- delimiter')
        if len(create_table_sql_parts) > 0:
            for i, sql in enumerate(create_table_sql_parts):
                normalized_sql = sql.strip()
                if i > 0 and normalized_sql != '':
                    with get_db().cursor() as cursor:
                        try:
                            cursor.execute(normalized_sql)
                        except Exception as e:
                            print(f"Error on create table for file {create_table_filename}.")
                            raise e
        
        print(f"{pref_config['TARGET_TABLE']} table created.")
    
    # Create the SP
    with open(create_sp_filename) as fp:
        create_sp_sql = fp.read()
    
    create_sp_sql_parts = create_sp_sql.split('GO -- delimiter')
    if len(create_sp_sql_parts) > 0:
        for i, sql in enumerate(create_sp_sql_parts):
            normalized_sql = sql.strip()
            if i > 0 and normalized_sql != '':
                with get_db().cursor() as cursor:
                    try:
                        cursor.execute(normalized_sql)
                    except Exception as e:
                        print(f"Error on create sp for file {create_sp_filename}.")
                        raise e

        sp_name = pref_config['STORED_PROCEDURE_NAME']
        print(f"{src.code_gen.utils.get_sp_name(pref_config['TARGET_TABLE'], sp_name)} stored procedure created.")
    
    # Create check merge SP
    with open(create_check_merge_sp_filename) as fp:
        create_check_merge_sp_sql = fp.read()
    
    create_check_merge_sp_sql_parts = create_check_merge_sp_sql.split('GO -- delimiter')
    if len(create_check_merge_sp_sql_parts) > 0:
        for i, sql in enumerate(create_check_merge_sp_sql_parts):
            normalized_sql = sql.strip()
            if i > 0 and normalized_sql != '':
                with get_db().cursor() as cursor:
                    try:
                        cursor.execute(normalized_sql)
                    except Exception as e:
                        print(f"Error on create check merge sp for file {create_check_merge_sp_filename}.")
                        raise e
        
        sp_name = f"CHECK_{pref_config['STORED_PROCEDURE_NAME']}"
        print(f"{src.code_gen.utils.get_sp_name(pref_config['TARGET_TABLE'], sp_name)} stored procedure created.")
    
    close()
