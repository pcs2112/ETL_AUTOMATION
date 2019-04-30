import ntpath
import src.preference_file_utils
import src.code_gen
import src.db_utils


def bulk_create_json_preference_files(in_filename):
    filename = src.preference_file_utils.get_configuration_file_path(in_filename)
    
    json_files = src.preference_file_utils.create_json_preference_files(filename)
    print('The following JSON preference files were created:')
    for json_file in json_files:
        print(json_file)
    
    data = src.preference_file_utils.get_excel_preference_file_data(filename)
    index_sql_files = []
    for config in data:
        search_column_index_exists = False
        search_column = config['SOURCE_TABLE_SEARCH_COLUMN']
        if search_column:
            try:
                search_column_index_exists = src.db_utils.column_index_exists(
                    config['SOURCE_SCHEMA'],
                    config['SOURCE_TABLE'],
                    search_column['column_name']
                )
            except Exception:
                pass
            
        if not search_column_index_exists:
            index_sql_files.append(src.code_gen.create_column_index(
                config['SOURCE_SCHEMA'],
                config['SOURCE_TABLE'],
                '' if search_column['column_name'] is None else search_column['column_name']['column_name'],
                config['ROW_COUNT'],
                config['ROW_MIN_DATE'],
                config['ROW_MAX_DATE'],
                config['MONTH_COUNT']
            ))
    
    print("")
    print('The following indices file was created:')
    print(src.code_gen.create_column_indices(index_sql_files))
    print("")
    print('Run the following commands to generate the DDL files:')
    for json_file in json_files:
        print(f"python app.py create_sp {ntpath.basename(json_file)}")
    
    print("")
