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
