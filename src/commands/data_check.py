import os
import src.data_check_utils
import src.excel_utils
import src.utils
from src.config import get_config
import pprint

pp = pprint.PrettyPrinter(indent=4)


def data_check(in_filename):
    filename = src.utils.get_configuration_file_path(in_filename)
    configs = src.data_check_utils.get_excel_preference_file_data(filename)

    sheets = []
    data = []
    
    for config in configs:
        sheet_data = src.data_check_utils.compare_tables(config)
        if len(sheet_data) > 0:
            sheets.append(config['SOURCE_TABLE'])
            data.append(sheet_data)
            
    if len(data) < 1:
        print("")
        print('Nothing to do. aborting.')
        exit(0)
        
    filename, file_extension = os.path.splitext(in_filename)
    out_filename = os.path.join(get_config()['ETL_CONFIG_OUT_DIR'], filename + '_DIFFS' + file_extension)
    src.excel_utils.write_workbook_data(out_filename, sheets, data)

    print("")
    print('The following diffs file was created:')
    print(out_filename)
    print("")
