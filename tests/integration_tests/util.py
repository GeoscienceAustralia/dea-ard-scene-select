"""
    Helper functions for integration tests
"""

from typing import List


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


def get_expected_file_paths(DATASETS: List) -> List:
    return [file_path.replace(".odc-metadata.yaml", ".tar") for file_path in DATASETS]
