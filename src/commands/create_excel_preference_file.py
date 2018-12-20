import ntpath
import src.preference_file_utils


def create_excel_preference_file(file_name):
	file = src.preference_file_utils.create_excel_preference_file(
		src.preference_file_utils.get_configuration_file_path(file_name)
	)
	print('The following file was created:')
	print(file)
	print("")
	print("Run the following command to generate the JSON preference files:")
	print(f"python app.py bulk_create_json_preference_files {ntpath.basename(file)}")
	print("")
