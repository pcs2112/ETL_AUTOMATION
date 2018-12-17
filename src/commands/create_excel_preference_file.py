import src.preference_file_utils


def create_excel_preference_file(file_name):
	file = src.preference_file_utils.create_excel_preference_file(
		src.preference_file_utils.get_configuration_file_path(file_name)
	)
	print('The following file was created:')
	print(file)
