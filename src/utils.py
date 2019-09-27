import os
import ntpath
from datetime import *
from src.config import get_config


def get_configuration_file_path(file_name):
    """
    Returns the path for the specified configuration file.
    :param str file_name:
    :return: str
    """
    file_path = os.path.join(get_config()['ETL_CONFIG_IN_DIR'], ntpath.basename(file_name))
    if os.path.exists(file_path) is False:
        raise FileExistsError(f"{file_name} is an invalid file.")

    return file_path


def get_configuration_file(file_name):
    """
    Returns the data for the specified configuration file.
    :param str file_name:
    :return: str
    """
    file_path = get_configuration_file_path(file_name)

    with open(file_path) as fp:
        contents = fp.read()

    return contents


def get_base_sql_code(file_name):
    """
    Returns the file contents in a string.
    :param str file_name:
    :return: str
    """
    file_path = os.path.join(get_config()['CODE_GENERATION_IN_DIR'], ntpath.basename(file_name))
    with open(file_path) as fp:
        contents = fp.read()
    
    return contents


def create_sql_file(file_name, sql_contents):
    """
    Writes and creates a sql file.
    :param str file_name: File name
    :param str sql_contents: The file's SQL contents
    :return: str New file path
    """
    file_path = os.path.join(get_config()['ETL_CONFIG_OUT_DIR'], ntpath.basename(file_name))
    with open(file_path, "w+") as fp:
        fp.write(sql_contents)
    
    return file_path


def split_string(value, delimiter=','):
    """ Splits the specified string and returns an array containing each part. """
    tmp_parts = value.split(delimiter)
    parts = []
    if len(tmp_parts) > 0:
        for tmp_part in tmp_parts:
            tmp_part = tmp_part.strip()
            if tmp_part != '':
                parts.append(tmp_part)
    
    return parts


def serialize_value_for_excel(value, delimiter=";"):
    """ Serializes an value into a string. """
    out_str = ''
    if isinstance(value, dict):
        for value in value.values():
            out_str += value + delimiter
        
        out_str = out_str.rstrip(delimiter)
    else:
        out_str = value
    
    return out_str


def serialize_list_for_excel(arr, delimiter="|"):
    """ Serializes a list into a string. """
    out_str = ''
    if len(arr) > 0:
        for value in arr:
            out_str += serialize_value_for_excel(value) + delimiter
    
    return out_str.rstrip(delimiter)


def get_date_str(obj, datetime_format='%Y-%m-%d %H:%M:%S'):
    """ Returns the date obj in a string format. """
    if isinstance(obj, datetime):
        return obj.strftime(datetime_format)
    elif isinstance(obj, date):
        return obj.strftime(datetime_format)
    else:
        return obj


def get_default_value(value, default):
    """ Returns a default value if a value is empty. """
    if isinstance(value, str):
        if value.strip() == '':
            return default
        
        return value
    
    if value is None:
        return default
    
    return value


def get_current_timestamp(format_str="%Y-%m-%d %H:%M"):
    """ Returns the current timestamp using the specified date format. """
    now = datetime.now()
    return now.strftime(format_str)


def get_filename_date_postfix():
    """ Returns a timestamp string append to file names. """
    return get_current_timestamp('%Y%m%d_%H%M%S')


def is_empty(value):
    """ Checks if a value is empty. """
    if isinstance(value, str):
        value = value.strip()
    
    return False if value else True
