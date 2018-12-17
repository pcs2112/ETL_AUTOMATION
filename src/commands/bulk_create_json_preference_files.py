import src.preference_file_utils


def bulk_create_json_preference_files(file_name):
	files = src.preference_file_utils.create_json_preference_files(
		src.preference_file_utils.get_configuration_file_path(file_name)
	)
	print('The following files were created:')
	for file in files:
		print(file)
