import xlrd
import xlwt


def read_workbook(file_path):
	""" Returns the workbook instance for the specified file. """
	return xlrd.open_workbook(file_path)


def read_workbook_columns(wb, sheet_index=0):
	""" Returns a list of columns for the specified workbook sheet. """
	sheet = wb.sheet_by_index(sheet_index)
	columns = []

	for i in range(sheet.ncols):
		columns.append(sheet.cell(0, i).value)

	return columns


def read_workbook_data(wb, sheet_index=0):
	"""
	Returns the specified workbook sheet's data as an array of objects
	using the header columns as the keys
	"""
	header_columns = read_workbook_columns(wb, sheet_index)
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


def write_workbook_data(filename, sheets, data):
	""" Writes the specified data into the workbook. """
	wb = xlwt.Workbook(encoding="UTF-8")

	for sheet_name in sheets:
		ws = wb.add_sheet(sheet_name)

		for i, row in enumerate(data):
			for x, cell in enumerate(row):
				ws.write_column(x, i, cell)

	wb.save(filename)
