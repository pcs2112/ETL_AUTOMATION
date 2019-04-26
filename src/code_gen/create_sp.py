import src.utils
import src.code_gen.utils

base_sql_file_name = 'create_sp.sql'
out_file_name_postfix = 'SP.sql'


def get_target_table_column_name(column_name, table_definition):
    norm_column_name = column_name.upper()
    if norm_column_name not in table_definition:
        return column_name

    return table_definition[norm_column_name]['target_table_column_name']


def get_insert_columns(table_definition, extra_columns):
    insert_columns = []
    for key, column in table_definition.items():
        insert_columns.append(f"[{column['target_table_column_name']}]")

    for column in extra_columns:
        insert_columns.append(f"[{get_target_table_column_name(column['column_name'], table_definition)}]")

    return insert_columns


def get_update_values(table_definition, extra_columns):
    update_values = []
    for key, column in table_definition.items():
        update_values.append(f"[{column['target_table_column_name']}] = source.[{column['target_table_column_name']}]")

    for column in extra_columns:
        update_values.append(
            f"[{get_target_table_column_name(column['column_name'], table_definition)}] = {column['value']}"
        )

    return update_values


def get_insert_values(table_definition, extra_columns):
    insert_values = []
    for key, column in table_definition.items():
        insert_values.append(f"source.[{column['target_table_column_name']}]")

    for column in extra_columns:
        insert_values.append(f"{column['value']}")

    return insert_values


def get_is_utc(table_definition, search_column):
    print(search_column)
    if search_column:
        if search_column['is_utc']:
            return True

        for key, column in table_definition.items():
            if column['column_name'] == search_column['column_name'] and 'utc' in column['column_name'].lower():
                return True

    return False


def get_update_match_check_columns_sql(table_definition, match_check_columns):
    conditions = []
    for key, column in table_definition.items():
        if column['column_name'] in match_check_columns:
            if column['is_nullable'] == 1:
                conditions.append(
                    f"(target.[{column['target_table_column_name']}] is NULL and source.[{column['target_table_column_name']}] is NOT NULL)"
                )
                conditions.append(
                    f"(target.[{column['target_table_column_name']}] is NOT NULL and source.[{column['target_table_column_name']}] is NULL)"
                )

            conditions.append(
                f"target.[{column['target_table_column_name']}] != source.[{column['target_table_column_name']}]")

    return " or \n".join([str(condition) for condition in conditions])


def get_pk_condition_sql(table_definition, primary_keys):
    pk_column_names = []
    for pk in primary_keys:
        pk_column_names.append(pk['column_name'])

    conditions = []
    for key, column in table_definition.items():
        if column['column_name'] in pk_column_names:
            conditions.append(
                f"target.[{column['target_table_column_name']}] = source.[{column['target_table_column_name']}]"
            )

    return " and \n".join([str(condition) for condition in conditions])


def get_search_condition(search_column, search_condition):
    if search_column:
        sql = src.utils.get_base_sql_code('search_condition_section.sql')
        sql = sql.replace('<SOURCE_TABLE_SEARCH_COLUMN>', search_column['column_name'])
    else:
        sql = '1 = 1 <SOURCE_TABLE_SEARCH_CONDITION>'

    sql = sql.replace('<SOURCE_TABLE_SEARCH_CONDITION>', search_condition)

    return sql


def create_sp(config, table_definition, table_counts=None):
    # Get the base sql for creating a table
    sql = src.utils.get_base_sql_code(base_sql_file_name)

    search_column = config['SOURCE_TABLE_SEARCH_COLUMN']

    # Get is UTC
    is_utc = get_is_utc(table_definition, search_column)

    # Set placeholder values
    target_table = src.code_gen.utils.get_target_table_name(config['SOURCE_TABLE'], config['TARGET_TABLE'])
    sql = sql.replace(
        '<STORED_PROCEDURE_SCHEMA>', src.code_gen.utils.get_sp_name(target_table, config['STORED_PROCEDURE_SCHEMA'])
    )
    sql = sql.replace(
        '<STORED_PROCEDURE_NAME>', src.code_gen.utils.get_sp_name(target_table, config['STORED_PROCEDURE_NAME'])
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

    if search_column:
        sql = sql.replace('<SOURCE_TABLE_SEARCH_COLUMN>', search_column['column_name'])

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
    sql = sql.replace('<SET_DAY_START>', str(1 if config['SET_DAY_START'] else 0))

    period_start_date = src.code_gen.utils.get_period_start_date(
        '' if table_counts is None else table_counts['min_value']
    )
    sql = sql.replace('<PERIOD_START_DATE>', period_start_date.strftime('%Y-%m-%d'))

    # Set the target table definition columns
    target_table_column_definition = src.code_gen.utils.get_target_table_definition(table_definition)
    target_table_column_definition_sql = ",\n".join(["\t\t" + str(column) for column in target_table_column_definition])
    sql = sql.replace('<TARGET_TABLE_COLUMN_DEFINITION>', target_table_column_definition_sql)

    # Set the source table definition columns
    source_table_column_definition = src.code_gen.utils.get_source_table_definition(table_definition)
    source_table_column_definition_sql = ",\n".join(["\t\t" + str(column) for column in source_table_column_definition])
    sql = sql.replace('<SOURCE_TABLE_COLUMN_DEFINITION>', source_table_column_definition_sql)

    # Set the target table column names TARGET_TABLE_COLUMN_NAMES
    target_table_column_names = src.code_gen.utils.get_target_table_column_names(table_definition)
    target_table_column_names_sql = ",\n".join(
        ["\t\t" + '[' + str(column) + ']' for column in target_table_column_names])
    sql = sql.replace('<TARGET_TABLE_COLUMN_NAMES>', target_table_column_names_sql)

    # Set the source table column names
    source_table_column_names = src.code_gen.utils.get_source_table_column_names(table_definition)
    source_table_column_names_sql = ",\n".join(
        ["\t\t" + '[' + str(column) + ']' for column in source_table_column_names])
    sql = sql.replace('<SOURCE_TABLE_COLUMN_NAMES>', source_table_column_names_sql)

    # Set the target table column names
    target_table_column_names = src.code_gen.utils.get_target_table_column_names(table_definition)
    target_table_column_names_sql = ",\n".join(
        ["\t\t" + '[' + str(column) + ']' for column in target_table_column_names])
    sql = sql.replace('<TARGET_TABLE_COLUMN_NAMES>', target_table_column_names_sql)

    # Set the pk condition
    sql = sql.replace('<TARGET_TABLE_PRIMARY_KEY_CONDITION>', get_pk_condition_sql(
        table_definition, config['SOURCE_TABLE_PRIMARY_KEY']
    ))

    # Set the target table update columns and values
    target_table_update_values = get_update_values(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
    target_table_update_values_sql = ",\n".join(["\t\t" + str(column) for column in target_table_update_values])
    sql = sql.replace('<TARGET_TABLE_UPDATE_VALUES>', target_table_update_values_sql)

    # Set the target table insert columns and values
    target_table_insert_columns = get_insert_columns(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
    target_table_insert_columns_sql = ",\n".join(["\t\t" + str(column) for column in target_table_insert_columns])
    sql = sql.replace('<TARGET_TABLE_INSERT_COLUMNS>', target_table_insert_columns_sql)

    target_table_insert_values = get_insert_values(table_definition, config['TARGET_TABLE_EXTRA_COLUMNS'])
    target_table_insert_values_sql = ",\n".join(["\t\t" + str(column) for column in target_table_insert_values])
    sql = sql.replace('<TARGET_TABLE_INSERT_VALUES>', target_table_insert_values_sql)

    # Set the target table search condition
    sql = sql.replace('<SOURCE_TABLE_SEARCH_CONDITION>', get_search_condition(
        search_column,
        config['SOURCE_TABLE_SEARCH_CONDITION']
    ))

    # Set the match check columns
    if len(config['UPDATE_MATCH_CHECK_COLUMNS']) > 0:
        sql = sql.replace('<MATCH_SECTION>', src.utils.get_base_sql_code('match_section.sql'))
        sql = sql.replace('<UPDATE_MATCH_CHECK_COLUMNS>', get_update_match_check_columns_sql(
            table_definition, config['UPDATE_MATCH_CHECK_COLUMNS']
        ))
    else:
        sql = sql.replace('<MATCH_SECTION>', src.utils.get_base_sql_code('match_section_empty.sql'))

    # Create the file and return its pathC8_<STORED_PROCEDURE_SCHEMA>.<STORED_PROCEDURE_NAME>
    return src.utils.create_sql_file(
        f"C8_{config['STORED_PROCEDURE_SCHEMA']}.{config['STORED_PROCEDURE_NAME']}_{out_file_name_postfix}", sql
    )
