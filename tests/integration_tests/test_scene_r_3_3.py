"""
    DSNS-231
       R3.3 Filter out the l1 scene if there is already an ARD scene with the
       same scene_id, that is un-archived.
"""
from pathlib import Path
import os
import json
from click.testing import CliRunner
import pytest
from scene_select.ard_scene_select import scene_select, GEN_LOG_FILE
from scene_select.do_ard import ODC_FILTERED_FILE
import datacube
from util import (
    get_list_from_file,
    get_config_file_contents,
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
        "c3/LC81150802019349/LC08_L1TP_115080_20191215_20191226_01_T1.odc-metadata.yaml",
    ),
    os.path.join(
        DATASETS_DIR,
        "c3/LC81150802019349/LC08_L1TP_115080_20191215_20201023_01_T1.odc-metadata.yaml",
    ),
    os.path.join(
        DATASETS_DIR,
        "c3/ARD_LC81150802019349_old/ga_ls8c_ard_3-1-0_115080_2019-12-15_final.odc-metadata.yaml",
    ),
]

def generate_temp_config_file(tmp_path):
    test_config_file = os.path.abspath(tmp_path / "config_file.conf")
    config_file_contents = get_config_file_contents()
    with open(test_config_file, "w", encoding="utf-8") as config_file_handler:
        config_file_handler.write(config_file_contents)
    config_file_handler.close()
    return test_config_file


pytestmark = pytest.mark.usefixtures("auto_odc_db")

def test_scene_r_3_3(odc_test_db: datacube.Datacube, tmp_path):
    """
    This is the collective test that implements the requirement as
    defined at the top of this test suite.
    """
    odc_test_db.index.datasets.archive(["760315b3-e147-5db2-bb7f-0e52efd4453d"])

    cmd_params = [
        "--products",
        '[ "usgs_ls8c_level1_1" ]',
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
    assert matching_files and matching_files[0] is not None, (
        "Scene select failed. List of entries to process is not available - ",
        f"{matching_files}",
    )
    ards_to_process = get_list_from_file(matching_files[0])

    # Given that the run should have no ards to process, we expect
    # an empty scenes_to_ARD_process.txt file.

    assert len(ards_to_process) == 0, (
        "Ard entries to process exist when we are not expecting anything",
        " to be there",
    )

    # Use glob to search for the log file
    # within filter-jobid-* directories
    matching_files = list(Path(tmp_path).glob("filter-jobid-*/" + GEN_LOG_FILE))

    # There's only ever 1 copy of this file
    assert (
        matching_files and matching_files[0] is not None
    ), f"Scene select failed. Log is not available - {matching_files}"

    found_log_line = False

    with open(matching_files[0], encoding="utf-8") as ard_log_file:
        for line in ard_log_file:
            try:
                jline = json.loads(line)
                if (
                    all(key in jline for key in ("reason", "event", "landsat_scene_id"))
                    and jline["reason"] == "The scene has been processed"
                    and jline["event"] == "scene removed"
                    and jline["landsat_scene_id"]
                    == "LC08_L1TP_115080_20191215_20201023_01_T1"
                ):
                    found_log_line = True
                    break
            except json.JSONDecodeError as error_string:
                print(f"Error decoding JSON: {error_string}")

    assert found_log_line, (
        "landsat scene still selected despite there is already an ARD scene",
        " with the same id and its initial scene had been archived",
    )
