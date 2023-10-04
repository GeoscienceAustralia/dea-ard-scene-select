"""
    DSNS-230
        R3.2 Process a scene if the child is interim and ancill data is there
"""
from pathlib import Path
from typing import List
import os
import subprocess
from click.testing import CliRunner
import pytest
from scene_select.ard_scene_select import scene_select, GEN_LOG_FILE
from scene_select.do_ard import ODC_FILTERED_FILE

from util import (
    get_list_from_file,
    generate_yamldir_value,
    get_config_file_contents,
)

METADATA_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "odc_setup", "metadata").resolve()
)
METADATA_TYPES = [
    os.path.join(METADATA_DIR, "eo3_sentinel.odc-type.yaml"),
    os.path.join(METADATA_DIR, "eo3_sentinel_ard.odc-type.yaml"),
]

PRODUCTS_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "odc_setup", "eo3").resolve()
)

PRODUCTS = [
    os.path.join(PRODUCTS_DIR, "esa_s2am_level1_0.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ga_s2am_ard_3.odc-product.yaml"),
]

DATASETS_DIR = (
    Path(__file__)
    .parent.joinpath(
        "..",
        "test_data",
        "integration_tests",
    )
    .resolve()
)

pytestmark = pytest.mark.usefixtures("auto_odc_db")

dataset_paths = [
    os.path.join(
        DATASETS_DIR,
        "s2/autogen/yaml/2022/2022-11/30S130E-35S135E/"
        + "S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932."
        + "odc-metadata.yaml",
    ),
    os.path.join(
        DATASETS_DIR,
        "c3/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T02193_ard/"
        + "ga_s2am_ard_3-2-1_53JMG_2022-11-23_final.odc-metadata.yaml",
    ),
]


def generate_commands(paths: List[str], tmp_path) -> str:
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
        Path: the path to the config file. note, not currently used.
          Keeping it here for potential future use.
    """

    config_file_contents = get_config_file_contents()

    # Get these environment values

    automated_test_config_file = os.environ.get("AUTOMATED_TEST_CONFIG_FILE")

    test_config_file = tmp_path / "config_file.conf"

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


def test_s2_normal_operation_r3_2(tmp_path):
    """
    This is the collective test that implements the requirement as
    defined at the top of this test suite.
    """

    the_cmds, _ = generate_commands(dataset_paths, tmp_path)

    # Run the command and capture its output
    result = subprocess.run(
        [the_cmds],
        shell=True,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    assert (
        result.returncode == 0
    ), f"The manual dataset addition failed: {result.stderr}"

    # These are for working out yamldir

    yamldir = generate_yamldir_value()

    cmd_params = [
        "--products",
        '[ "esa_s2am_level1_0" ]',
        "--yamls-dir",
        yamldir,
        "--logdir",
        tmp_path,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        args=cmd_params,
    )

    assert (
        result.exit_code == 0
    ), f"The scene_select process failed to execute: {result.output}"
    assert result.output != "", f" the result output is {result.output}"

    # Use glob to search for the log file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + GEN_LOG_FILE))

    # There's only ever 1 copy of this file
    assert (
        matching_files and matching_files[0] is not None
    ), f"Scene select failed. Log is not available - {matching_files}"

    assert matching_files and matching_files[0] is not None, (
        "Scene select failed. List of entries to process is not available -",
        f" {matching_files}",
    )

    # Use glob to search for the scenes_to_ARD_process.txt file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + ODC_FILTERED_FILE))

    # There's only ever 1 copy of scenes_to_ARD_process.txt after
    # successfully processing
    assert matching_files and matching_files[0] is not None, (
        "Scene select failed. List of entries to process is not available : "
        f"{ODC_FILTERED_FILE} - {matching_files}"
    )

    ards_to_process = get_list_from_file(matching_files[0])
    assert (
        len(ards_to_process) == 1
    ), "Expected only 1 zip files to process but this has not been the case"

    expected_file = (
        "/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2022"
        + "/2022-11/30S130E-35S135E"
        + "/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.zip"
    )

    assert (
        ards_to_process[0].endswith(".zip") is True
    ), f"The generated ard file name, '{ards_to_process[0]}' doesn't end with zip"

    assert (
        ards_to_process[0] == expected_file
    ), "The generated ard file path is not what is expected"
