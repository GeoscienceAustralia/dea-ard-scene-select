"""
    Helper functions for integration tests
"""

from typing import List, Optional
import os

def get_list_from_file(list_file: str) -> List:
    """
        Reads each line of the given list file
        and returns a list of strings.

    Parameters:
    - list_file: The path to the list file to be read.

    Returns:
    - List: A list of strings, each string representing line content
        of the input file
    """
    with open(list_file, "r", encoding="utf-8") as file:
        file_list = [line.strip() for line in file]

    return file_list


"""
    Generate a list of expected file paths by replacing
    '.odc-metadata.yaml' with '.tar'
    for each file path in a given list.

    Parameter(s):
    - List: A list of file paths containing '.odc-metadata.yaml' files.

    Returns:
    - List: A list of file paths with '.tar' extensions, corresponding
        to the input DATASETS.

"""


def get_expected_file_paths(datasets: List, dataset_type: Optional[str] = "landsat") -> List:

    output_file_extension = ".tar"
    if dataset_type == "s2":
        output_file_extension = ".zip"
    return [
        file_path.replace(".odc-metadata.yaml", output_file_extension)
        for file_path in datasets
    ]

def generate_yamldir_value():
    """
    Generate the path to the YAML directory based on the script directory.

    Returns:
        str: A string representing the YAML directory path.
    """

    script_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    return f" {os.path.join(script_directory, 'test_data/integration_tests/s2/autogen/yaml')}"
