"""
DSNS-239
R3.1 Process a scene if the ancillary is not there,
after the wait time (Process to interim)
"""

from collections import Counter
from pathlib import Path
import os
import json
from click.testing import CliRunner
import pytest
from scene_select.ard_scene_select import scene_select, GEN_LOG_FILE
from scene_select.do_ard import ODC_FILTERED_FILE

from util import (
    get_list_from_file,
    get_expected_file_paths,
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
    os.path.join(PRODUCTS_DIR, "l1_ls8.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "l1_ls8_c2.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ard_ls8.odc-product.yaml"),
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
        "c3/LC81020792023029_do_interim/LC08_L1GT_102079_20230129_20230227_02_T2.odc-metadata.yaml",
    ),
]

pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_interim_prod_r3_1(tmp_path):
    """
    This is the collective test that implements the requirement as
    defined at the top of this test suite.
    """

    # Observe that there is a date exclusion filter passed
    # into the process
    cmd_params = [
        "--products",
        '[ "usgs_ls8c_level1_2" ]',
        "--logdir",
        tmp_path,
        "--allowed-codes",
        "Australian_AOI_107069_added.json",
        "--days-to-exclude",
        '["2009-01-03:2009-01-05"]',
        "--interim-days-wait",
        5,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        args=cmd_params,
    )

    assert result.exit_code == 0, "The scene_select process failed to execute"

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

    found_log_line = False
    with open(matching_files[0], encoding="utf-8") as ard_log_file:
        for line in ard_log_file:
            try:
                jline = json.loads(line)
                if (
                    all(key in jline for key in ("event", "landsat_scene_id", "level"))
                    and "Processing to interim" in jline["event"]
                    and jline["landsat_scene_id"]
                    == "LC08_L1GT_102079_20230129_20230227_02_T2"
                    and jline["level"] == "debug"
                ):
                    found_log_line = True
                    break
            except json.JSONDecodeError as error_string:
                print(f"Error decoding JSON: {error_string} in line:{line}")
    assert (
        found_log_line
    ), "Landsat scene still selected despite its date is being excluded"

    # Use glob to search for the scenes_to_ARD_process.txt file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + ODC_FILTERED_FILE))

    # There's only ever 1 copy of scenes_to_ARD_process.txt after
    # successfully processing
    assert matching_files and matching_files[0] is not None, (
        f"Scene select failed. List of entries to process is not available :{ODC_FILTERED_FILE} "
        f"- {matching_files}"
    )
    ards_to_process = get_list_from_file(matching_files[0])
    expected_files = get_expected_file_paths(DATASETS)
    assert Counter(ards_to_process) == Counter(
        expected_files
    ), "Lists do not have the same contents."
