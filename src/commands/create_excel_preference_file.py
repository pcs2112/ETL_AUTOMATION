import ntpath
import src.preference_file_utils
import src.utils


def create_excel_preference_file(in_filename):
	out_filename = src.preference_file_utils.create_excel_preference_file(
		src.preference_file_utils.get_configuration_file_path(in_filename)
	)

	src.utils.print_green('The following file was created:')
	src.utils.print_yellow(out_filename)
	print("")
	src.utils.print_green("Run the following command to generate the JSON preference files:")
	src.utils.print_yellow(f"python app.py bulk_create_json_preference_files {ntpath.basename(out_filename)}")
	print("")
