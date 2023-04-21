from typing import Dict


def get_file_paths_by_city_and_type(data_path: str) -> Dict[str, Dict[str, str]]:
    """ Utility to build a dictionary of data file paths for the client

    :param data_path: Path to the `data` directory, which should contain directories for each city with .csv files
    :return: A dictionary with keys as cities and with values as a dictionary of (file_type, file_path)
    """
    from os import walk, path
    from collections import defaultdict
    files_paths_by_city_and_type = defaultdict(dict)
    for dir_name, _, files in walk(top=data_path):
        for file in files:
            city = path.basename(dir_name)
            file_type = path.splitext(file)[0]
            files_paths_by_city_and_type[city][file_type] = path.join(dir_name, file)
    return files_paths_by_city_and_type

