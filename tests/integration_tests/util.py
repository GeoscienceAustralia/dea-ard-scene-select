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


def get_expected_file_paths(DATASETS: List) -> List:
    return [file_path.replace(".odc-metadata.yaml", ".tar") for file_path in DATASETS]


def generate_yamldir_value():
    """
    Generate the path to the YAML directory based on the script directory.

    Returns:
        str: A string representing the YAML directory path.
    """

    script_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    return f" {os.path.join(script_directory, 'test_data/integration_tests/s2/autogen/yaml')}"


def get_config_file_contents():
    """
    Create the temporary config file so that ard scene select
    processes that get runned in this script as subprocesses
    will be able to access the same datacube instance"""

    user_id = os.getenv("USER")
    return f"""
[datacube]
db_hostname: deadev.nci.org.au
db_port: 5432
db_database: {user_id}_automated_testing
"""


def generate_commands_and_config_file_path(paths: List[str], tmp_path) -> str:
    """
    Generate a group of shell commands that adds datasets to
    the current datacube we are using to test.
    This involves including environment variable settings
    and dataset addition commands for each dataset path.
    The reason this is done is because the s2 datasets
    are not supported properly in pytest-odc at the
    time this test is written.

    Returns:
        str: a long string comprising of multiple
        shell commands as described above
        str: the path to the config file. Note: not currently used.
          Keeping it here for potential future use.
    """

    config_file_contents = get_config_file_contents()

    automated_test_config_file = os.environ.get("AUTOMATED_TEST_CONFIG_FILE")

    test_config_file = os.path.abspath(tmp_path / "config_file.conf")

    with open(test_config_file, "w") as text_file:
        text_file.write(config_file_contents)

    datacube_add_command = ""
    for dpath in paths:
        datacube_add_command = (
            datacube_add_command
            + f"  datacube --config {test_config_file} "
            + f" dataset add --confirm-ignore-lineage {dpath}; "
        )

    return datacube_add_command, test_config_file
