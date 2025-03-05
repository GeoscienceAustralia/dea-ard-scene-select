"""
DSNS-229
    R1.1 for s2: Unfiltered scenes are ARD processed
"""

from pathlib import Path
import os
import subprocess
from click.testing import CliRunner
import pytest
from scene_select.ard_scene_select import scene_select
from scene_select.do_ard import ODC_FILTERED_FILE

from util import (
    get_list_from_file,
    generate_yamldir_value,
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

dataset_path = os.path.join(
    DATASETS_DIR,
    "s2/autogen/yaml/2022/2022-01/"
    + "15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH"
    + "_20220124T021536.odc-metadata.yaml",
)


def test_s2_normal_operation_r1_1(tmp_path):
    """
    This is the collective test that implements the requirement as
    defined at the top of this test suite.
    """
    cmd = f"datacube dataset add --confirm-ignore-lineage {dataset_path};"

    # Run the command and capture its output
    result = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    assert result.returncode == 0, (
        f"The manual dataset addition failed: {result.stderr}"
    )

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

    assert result.exit_code == 0, (
        f"The scene_select process failed to execute: {result.output}"
    )
    assert result.output != "", f" the result output is {result.output}"

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
    assert len(ards_to_process) == 1, (
        "Expected only 1 zip files to process but this has not been the case"
    )

    expected_file = (
        "/g/data/u46/users/dsg547/test_data/c3/s2_autogen/zip/15S140E-20S145E"
        + "/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip"
    )

    assert ards_to_process[0] == expected_file, (
        "The generated ard file path is not what is expected"
    )
