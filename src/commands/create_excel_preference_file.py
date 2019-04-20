import ntpath
import src.preference_file_utils


def create_excel_preference_file(in_filename):
    out_filename = src.preference_file_utils.create_excel_preference_file(
        src.preference_file_utils.get_configuration_file_path(in_filename)
    )
    
    print('The following file was created:')
    print(out_filename)
    print("")
    print("Run the following command to generate the JSON preference files:")
    print(f"python app.py bulk_create_json_preference_files {ntpath.basename(out_filename)}")
    print("")
