import xlrd
from .utils import get_configuration_file


def get_workbook(file_name):
	""" Returns the workbook instance for the specified file. """
	return xlrd.open_workbook(get_configuration_file(file_name), on_demand=True)


def get_workbook_columns(wb, sheet_index=0):
	""" Returns a list of columns for the specified workbook sheet. """
	sheet = wb.sheet_by_index(sheet_index)
	columns = []

	for i in range(sheet.ncols):
		columns.append(sheet.cell_value(0, i))

	return columns


def get_workbook_data(wb, sheet_index):
	"""
	Returns the specified workbook sheet's data as an array of objects
	using the header columns as the keys
	"""
	header_columns = get_workbook_columns(wb, sheet_index)
	num_columns = wb.ncols
	num_rows = wb.nrows
	data = []

	for row in range(1, num_rows):
		obj = {}
		for col in range(num_columns):
			obj[header_columns[col]] = wb.cell_value(row, col)

		data.append(obj)

	return data

