"""
    DSNS-241
    R1.1 for ls: Unfiltered scenes are ARD processed
    The tar is from /g/data/da82/AODH/USGS/L1/Landsat/C1/092_085/LC80920852020223
"""
from collections import Counter
from pathlib import Path
from typing import List
from click.testing import CliRunner
import pytest
import os
from scene_select.ard_scene_select import (
    scene_select,
)

from util import (
    get_list_from_file,
)  # TODO - Speak with Duncan. 'lib/' is in the .gitignore. Do we want to make an exception?


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
    os.path.join(PRODUCTS_DIR, "l1_ls8.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "l1_ls8_c2.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "l1_ls9.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ard_ls8.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ard_ls7.odc-product.yaml"),
    os.path.join(PRODUCTS_DIR, "ard_ls9.odc-product.yaml"),
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
        "c3/LC80920852020223_good/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml",
    ),
    os.path.join(
        DATASETS_DIR,
        "c3/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.odc-metadata.yaml",
    ),
    os.path.join(
        DATASETS_DIR,
        "c3/LC80970752022215/LC08_L1TP_097075_20220803_20220805_02_T1.odc-metadata.yaml",
    ),
]


def get_expected_file_paths() -> List:
    TEST_DATA_DIR = (
        Path(__file__)
        .parent.joinpath(
            "..",
            "test_data",
            "integration_tests",
        )
        .resolve()
    )
    return [
        os.path.join(
            TEST_DATA_DIR,
            "c3/LC80920852020223_good/LC08_L1TP_092085_20200810_20200821_01_T1.tar",
        ),
        os.path.join(
            TEST_DATA_DIR,
            "c3/LC80970752022215/LC08_L1TP_097075_20220803_20220805_02_T1.tar",
        ),
        os.path.join(
            TEST_DATA_DIR,
            "c3/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.tar",
        ),
    ]


pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_ard_landsat_unfiltered_scenes_r1_1(setup_local_directories_and_files):
    (scratch_dir, package_dir) = setup_local_directories_and_files

    cmd_params = [
        "--products",
        '["usgs_ls7e_level1_1", "usgs_ls8c_level1_1", "usgs_ls8c_level1_2", "usgs_ls9c_level1_2"]',
        "--workdir",
        scratch_dir,
        "--pkgdir",
        package_dir,
        "--logdir",
        scratch_dir,
        "--project",
        "u46",
        "--walltime",
        "02:30:00",
        "--interim-days-wait",
        5,
        "--days-to-exclude",
        '["2009-01-03:2009-01-05"]',
        "--index-datacube-env",
        "index-test-odc.env",
        # Uncomment the line below for '--run-ard' argument
        # "--run-ard",
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        args=cmd_params,
    )

    print("***** results output ******")
    print(result.output)
    print("***** results exception ******")
    print(result.exception)
    print("***** results end ******")

    assert result.exit_code == 0, "The scene_select process failed to execute"

    # Use glob to search for the file within filter-jobid-* directories
    matching_files = list(
        Path(scratch_dir).glob("filter-jobid-*/scenes_to_ARD_process.txt")
    )

    # There's only ever 1 copy of scenes_to_ARD_process.txt after
    # successfully processing
    assert (
        matching_files and matching_files[0] is not None
    ), f"Scene select failed. List of entries to process is not available - {matching_files}"
    ards_to_process = get_list_from_file(matching_files[0])

    expected_files = get_expected_file_paths()
    assert Counter(ards_to_process) == Counter(
        expected_files
    ), "Lists do not have the same contents."
