"""
DSNS-240
    R2.8.2 Filter out S2 l1 scenes if the dataset has a child,
    the child is interim and there is no ancillary
"""

from pathlib import Path
import os
import subprocess
import json
from click.testing import CliRunner
import pytest
from scene_select.ard_scene_select import scene_select, GEN_LOG_FILE
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
    os.path.join(PRODUCTS_DIR, "esa_s2bm_level1_0.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ga_s2am_ard_3.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ga_s2bm_ard_3.odc-product.yaml"),
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

s2_datasets = [
    os.path.join(
        DATASETS_DIR,
        (
            "s2/autogen/yaml/2022/2022-12/"
            "30S130E-35S135E/"
            "S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408"
            ".odc-metadata.yaml"
        ),
    ),
    os.path.join(
        DATASETS_DIR,
        (
            "c3/S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408_ard"
            "/ga_s2am_ard_3-2-1_53HMC_2022-12-03_interim.odc-metadata.yaml"
        ),
    ),
]

pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_scene_filtering_r2_8_2(tmp_path):
    """
    This is the collective test that implements the requirement as
    defined at the top of this test suite.

    Note: s2 datasets
    are not fully supported in pytest-odc at the
    time this test is written. So do a datacube dataset add call
    """

    cmds = "datacube dataset add --confirm-ignore-lineage "
    lines = [f"{cmds}{dpath}" for dpath in s2_datasets]
    # Run the command and capture its output
    result = subprocess.run(
        ";".join(lines),
        shell=True,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    assert (
        result.returncode == 0
    ), f"The manual dataset addition failed: {result.stderr}"

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

    found_log_line = False
    with open(matching_files[0], encoding="utf-8") as ard_log_file:
        for line in ard_log_file:
            try:
                jline = json.loads(line)
                if (
                    all(key in jline for key in ("reason", "dataset_id", "event"))
                    and jline["reason"] == "The scene has been processed"
                    and jline["dataset_id"] == "6a446ae9-7b10-544f-837b-c55b65ec7d68"
                    and jline["event"] == "scene removed"
                ):
                    found_log_line = True
                    break
            except json.JSONDecodeError as error_string:
                print(f"Error decoding JSON: {error_string}")
    assert found_log_line, f"Logs look wrong {matching_files[0]}"

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

    # Given that the run should have no ards to process, we expect
    # an empty scenes_to_ARD_process.txt file.

    assert len(ards_to_process) == 0, (
        "Ard entries to process exist when we are not expecting "
        f"anything to be there, {ards_to_process}"
    )
