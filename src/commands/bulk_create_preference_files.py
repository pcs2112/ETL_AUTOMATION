import src.preference_file_utils


def bulk_create_preference_files(file_name):
	files = src.preference_file_utils.create_preference_files(file_name)
	print('The following files were created:')
	for file in files:
		print(file)
