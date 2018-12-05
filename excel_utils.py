import xlrd


def get_workbook(file_path):
	""" Returns the workbook instance for the specified file. """
	return xlrd.open_workbook(file_path)


def get_workbook_columns(wb, sheet_index=0):
	""" Returns a list of columns for the specified workbook sheet. """
	sheet = wb.sheet_by_index(sheet_index)
	columns = []

	for i in range(sheet.ncols):
		columns.append(sheet.cell(0, i).value)

	return columns


def get_workbook_data(wb, sheet_index=0):
	"""
	Returns the specified workbook sheet's data as an array of objects
	using the header columns as the keys
	"""
	header_columns = get_workbook_columns(wb, sheet_index)
	sheet = wb.sheet_by_index(sheet_index)
	num_columns = sheet.ncols
	num_rows = sheet.nrows
	data = []

	for row in range(1, num_rows):
		obj = {}
		for col in range(num_columns):
			obj[header_columns[col]] = sheet.cell(row, col).value

		data.append(obj)

	return data
