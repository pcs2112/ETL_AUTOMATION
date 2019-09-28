import os
import src.compare_tables_utils
import src.excel_utils
import src.utils
from src.config import get_config
import pprint

pp = pprint.PrettyPrinter(indent=4)


def compare_tables(in_filename):
    filename = src.utils.get_configuration_file_path(in_filename)
    configs = src.compare_tables_utils.get_excel_preference_file_data(filename)

    sheets = []
    data = []
    
    for config in configs:
        sheet_data = src.compare_tables_utils.compare_tables(config)
        if len(sheet_data) > 0:
            sheets.append(config['SOURCE_TABLE'])
            data.append(sheet_data)

    filename, file_extension = os.path.splitext(in_filename)
    out_filename = os.path.join(get_config()['ETL_CONFIG_OUT_DIR'], filename + '_DIFFS' + file_extension)
    src.excel_utils.write_workbook_data(out_filename, sheets, data)

    print("")
    print('The following diffs file was created:')
    print(out_filename)
    print("")
