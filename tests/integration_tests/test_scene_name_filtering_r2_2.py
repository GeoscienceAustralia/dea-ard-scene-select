"""
DSNS-232
R2.2 for landsat - Filter out scenes that do not match the product pattern

"""

from pathlib import Path
from click.testing import CliRunner
import pytest
import os
import json
from scene_select.ard_scene_select import scene_select, GEN_LOG_FILE
from scene_select.do_ard import ODC_FILTERED_FILE

from util import (
    get_list_from_file,
)


METADATA_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "odc_setup", "metadata").resolve()
)
METADATA_TYPES = [
    os.path.join(METADATA_DIR, "eo3_landsat_l1.odc-type.yaml"),
    os.path.join(METADATA_DIR, "eo3_landsat_ard.odc-type.yaml"),
]

PRODUCTS_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "odc_setup", "eo3").resolve()
)
PRODUCTS = [
    os.path.join(PRODUCTS_DIR, "l1_ls7.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ard_ls7.odc-product.yaml"),
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
DATASETS = [
    os.path.join(
        DATASETS_DIR,
        "c3/LE71080732020343_level_too_low/LE07_L1GT_108073_20201208_20210103_01_T2.odc-metadata.yaml",
    ),
]


pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_ard_landsat_scenes_not_matching_product_patterns_r2_2(tmp_path):
    cmd_params = [
        "--products",
        '["usgs_ls7e_level1_1"]',
        "--logdir",
        tmp_path,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        args=cmd_params,
    )

    assert result.exit_code == 0, "The scene_select process failed to execute"

    # Use glob to search for the scenes_to_ARD_process.txt file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + ODC_FILTERED_FILE))

    # There's only ever 1 copy of scenes_to_ARD_process.txt after
    # successfully processing
    assert (
        matching_files and matching_files[0] is not None
    ), f"Scene select failed. List of entries to process is not available - {matching_files}"
    ards_to_process = get_list_from_file(matching_files[0])

    # Given that the run should have no ards to process, we expect
    # an empty scenes_to_ARD_process.txt file.

    assert (
        len(ards_to_process) == 0
    ), "Ard entries to process exist when we are not expecting anything to be"

    # Use glob to search for the log file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + GEN_LOG_FILE))

    # There's only ever 1 copy of this file
    assert (
        matching_files and matching_files[0] is not None
    ), f"Scene select failed. Log is not available - {matching_files}"
    ard_logs = get_list_from_file(matching_files[0])

    found_log_line = False
    with open(matching_files[0]) as f:
        for line in f:
            jline = json.loads(line)
            if "reason" in jline and jline["reason"] == "Processing level too low":
                found_log_line = True
                break
    assert found_log_line, "Processing level too low not found in log file"
