"""
Test Suite for ARD Level 1 Reprocessing

This test suite contains tests that ensure the functionality of the ARD level 1
reprocessing process can be properly tested.

The tests cover the following main components:
- Executing the ARD reprocessed_l1s function.
- Moving blocked scenes and updating their paths.

Test Environment Setup:
The tests are executed using the pytest framework and utilize various fixtures
to set up the necessary test environment.

How this works:
   Pytest (in particular, pytest-odc will ensure a test db is set up for
   the test session) and datacube.
   A run of the level 1 ard reprocessing process will be done by
   using CliRunner (from click.testing) to simulate a call to the level 1
   ard reprocessing process just like how we run it in production.
   Assertions are then done on the resulting state.

Helper Functions:
- get_blocked_scene_ard: Returns the UUID of a blocked ARD scene
  for testing.
- get_file_paths_for_test_move_blocked: Returns a dictionary of
  file paths used in the move_blocked test.
"""

from pathlib import Path
import os
import pytest
from click.testing import CliRunner
import os.path
import uuid
import shutil
from subprocess import STDOUT
from typing import Dict

import datacube
from scene_select.ard_reprocessed_l1s import (
    ard_reprocessed_l1s,
    DIR_TEMPLATE,
    move_blocked,
)
from scene_select.do_ard import ARCHIVE_FILE, ODC_FILTERED_FILE, PBS_ARD_FILE

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ODC_YAML_DIR = Path(__file__).parent.joinpath("..", "test_data", "odc_setup").resolve()
SCENES_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
)

METADATA_TYPES = [
    ODC_YAML_DIR / "eo3_landsat_l1.odc-type.yaml",
    ODC_YAML_DIR / "eo3_landsat_ard.odc-type.yaml",
]
PRODUCTS = [
    ODC_YAML_DIR / "l1_ls9.odc-product.yaml",
    ODC_YAML_DIR / "ga_ls9c_ard_3.odc-product.yaml",
]

REPROCESS_TEST_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
)

# two l1 scenes and one ard scene
# the first l1 to be archived in the fixture
DATASETS = [
    REPROCESS_TEST_DIR
    / "l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
    "/g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml",
    "/g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "l1_Landsat_C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml",
    REPROCESS_TEST_DIR
    / "ga_ls9c_ard_3/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml",
]


@pytest.fixture(scope="session", autouse=True)
def setup_local_directories_and_files():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_rel = os.path.join(script_dir, "..", "test_data", "ls9_reprocessing")
    test_data = os.path.realpath(test_data_rel)

    if "gadi" in os.environ.get("HOSTNAME", ""):
        print("gadi - NCI")
        module_paths = [
            "/g/data/v10/public/modules/modulefiles",
            "/g/data/v10/private/modules/modulefiles",
        ]
        for module_path in module_paths:
            os.environ["MODULEPATH"] = os.pathsep.join(
                [module_path, os.environ.get("MODULEPATH", "")]
            )

        # Load the required module
        os.system("module load dea/20221025")
    else:
        print("Not on NCI")

    ss_path = os.path.abspath(os.path.join(script_dir, "..", ".."))

 
    # Delete and recreate the file structure
    test_data_moved = os.path.join(test_data, "moved", "ga_ls9c_ard_3")
    test_data_ga = os.path.join(test_data, "ga_ls9c_ard_3")
    test_data_raw = os.path.join(test_data, "a_ga_ls9c_ard_3_raw")
    shutil.rmtree(test_data_moved, ignore_errors=True)
    shutil.rmtree(test_data_ga, ignore_errors=True)
    shutil.copytree(test_data_raw, test_data_ga)
    os.makedirs(os.path.join(test_data, "moved"), exist_ok=True)

    script_dir_scratch = os.path.join(script_dir, "scratch")
    os.makedirs(script_dir_scratch, exist_ok=True)

    yield  # Let the tests begin ;)


@pytest.fixture
def archive(odc_test_db):
    for entry in [
        "4c68b81a-23a0-5e57-b983-96439fc4518c",
        "d530018e-5dad-58c2-8471-15f17d506604",
    ]:
        odc_test_db.index.datasets.archive([entry])


pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_ard_reprocessed_l1s(archive, odc_test_db: datacube.Datacube):
    """Test the ard_reprocessed_l1s function."""

    # Set this to true and all the results
    # of the scene select run will be displayed
    SCENE_SELECT_PROCESS_RUN_VERBOSE = False

    new_base_path = REPROCESS_TEST_DIR.joinpath("moved")
    current_base_path = REPROCESS_TEST_DIR
    SCRATCH_DIR = Path(__file__).parent.joinpath("scratch")

    # in bash
    # hex=$(openssl rand -hex 3)
    jobdir = SCRATCH_DIR.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    dry_run = False
    product = "ga_ls9c_ard_3"

    scene_limit = 2
    run_ard = False

    cmd_params = [
        "--current-base-path",
        str(current_base_path.resolve()),
        "--new-base-path",
        str(new_base_path.resolve()),
        # We need to provide our explicit jobdir so that
        # we can get hold of the ard to process file
        "--jobdir",
        str(jobdir),
        "--product",
        product,
        "--scene-limit",
        scene_limit,
        "--logdir",
        SCRATCH_DIR,
    ]

    try:
        runner = CliRunner()
        result = runner.invoke(ard_reprocessed_l1s, cmd_params)
        if SCENE_SELECT_PROCESS_RUN_VERBOSE:
            print("***** results output ******")
            print(result.output)
            print("***** results exception ******")
            print(result.exception)
            print("***** results end ******")

        # Assert a few things
        # Two dirs have been moved.  These are the previous datasets
        # that we sent in for reprocessing.

        new_dir_06_21 = REPROCESS_TEST_DIR.joinpath(
            "moved", "ga_ls9c_ard_3", "092", "081", "2022", "06", "21"
        )
        fname_06_21 = new_dir_06_21.joinpath(
            "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml"
        )

        assert os.path.isfile(
            fname_06_21
        ), f"The yaml file, '{fname_06_21}' has been moved, for a different scene"

        new_dir_06_27 = REPROCESS_TEST_DIR.joinpath(
            "moved", "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
        )
        yaml_fname_06_27 = new_dir_06_27.joinpath(
            "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
        )

        assert os.path.isfile(
            yaml_fname_06_27
        ), f"The yaml file, '{yaml_fname_06_27}' has been moved"

        ard_id_06_27 = "d9a499d1-1abd-4ed1-8411-d584ca45de25"
        ard_dataset = odc_test_db.index.datasets.get(ard_id_06_27)
        local_path = Path(ard_dataset.local_path).resolve()
        assert str(local_path) == str(
            yaml_fname_06_27
        ), "The OCD ARD path has not been updated"

        # uuids have been written to an archive file
        filename = jobdir.joinpath(ARCHIVE_FILE)

        assert os.path.isfile(
            filename
        ), f"There is no UUIDs archive list file, {filename}"
        with open(filename, "r", encoding="utf-8") as f:
            temp = f.read().splitlines()

        assert sorted(
            [
                "3de6cb49-60da-4160-802b-65903dcbbac8",
                "d9a499d1-1abd-4ed1-8411-d584ca45de25",
            ]
        ) == sorted(temp), "The correct uuids have been written to the archive file"

        odc_filename = jobdir.joinpath(ODC_FILTERED_FILE)

        with open(odc_filename, "r", encoding="utf-8") as f:
            temp = f.read().splitlines()

        if "HOSTNAME" in os.environ and "gadi" in os.environ["HOSTNAME"]:
            base_location = Path("/g/data/da82/AODH/USGS/L1/Landsat/C2/")
        else:
            base_location = REPROCESS_TEST_DIR.joinpath("l1_Landsat_C2")

        a_l1_tar = base_location.joinpath(
            "092_081",
            "LC90920812022172",
            "LC09_L1TP_092081_20220621_20220802_02_T1.tar",
        )
        b_l1_tar = base_location.joinpath(
            "102_076",
            "LC91020762022178",
            "LC09_L1TP_102076_20220627_20220802_02_T1.tar",
        )

        assert os.path.isfile(
            filename
        ), f"ard to be processed reference file {filename} does not exist"
        assert os.path.isfile(a_l1_tar), f"{a_l1_tar} file does not exist"
        assert os.path.isfile(b_l1_tar), f"{b_l1_tar} file does not exist"
        left = sorted([str(a_l1_tar), str(b_l1_tar)])
        right = sorted(temp)
        assert sorted([str(a_l1_tar), str(b_l1_tar)]) == sorted(temp), (
            "The correct l1 tars have been written to the scene select"
            " file, scenes_to_ARD_process.txt"
        )
        filename = jobdir.joinpath(PBS_ARD_FILE)
        assert os.path.isfile(filename), "There is a run ard pbs file"
    except Exception as exception_message:
        # this traps errors thrown out by the scene_select() function
        # such as bad parameter types or NoneType when the argument
        # is expected to be non-None
        pytest.fail(f"Unexpected exception: {exception_message}")
    finally:
        shutil.rmtree(jobdir)


def get_blocked_scene_ard() -> str:
    # ard_id_06_27
    return "d9a499d1-1abd-4ed1-8411-d584ca45de25"


def get_file_paths_for_test_move_blocked() -> Dict:
    return {
        "old_yaml_fname_06_27": REPROCESS_TEST_DIR.joinpath(
            "ga_ls9c_ard_3",
            "102",
            "076",
            "2022",
            "06",
            "27",
            "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
        ),
        "current_base_path": REPROCESS_TEST_DIR,
        "new_base_path": REPROCESS_TEST_DIR.joinpath("moved"),
        "yaml_fname_06_27": REPROCESS_TEST_DIR.joinpath(
            "moved",
            "ga_ls9c_ard_3",
            "102",
            "076",
            "2022",
            "06",
            "27",
            "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
        ),
    }


def test_move_blocked(archive, odc_test_db: datacube.Datacube):
    """
    Test the move_blocked function.
    This one does not involve running the
    ard reprocess l1s process
    """

    file_paths = get_file_paths_for_test_move_blocked()
    # Test the move_blocked function
    # for a 'normal' case
    blocked_scenes = [
        {
            "blocking_ard_id": get_blocked_scene_ard(),
            "blocked_l1_zip_path": "not used",
            "blocking_ard_path": file_paths["old_yaml_fname_06_27"],
        }
    ]

    l1_zips, uuids2archive = move_blocked(
        blocked_scenes,
        file_paths["current_base_path"],
        file_paths["new_base_path"],
    )

    # Test that in the case that
    # a move_blocked has occurred, but the
    # ARD is still blocking
    #  reprocessing will still occur
    # since the l1 scene is in the l1 zip list
    # Assert the dir has been moved

    # The yaml file has been moved
    assert os.path.isfile(
        file_paths["yaml_fname_06_27"]
    ), "The new yaml file is not found in the expected (new) location"

    #  There should be one l1 zip
    assert len(l1_zips) == 1, "Wrong count of level 1 zip file"
    # There should be one uuid to archive
    assert len(uuids2archive) == 1, "Wrong number of UUID to archive detected"

    ard_dataset = odc_test_db.index.datasets.get(get_blocked_scene_ard())
    local_path = Path(ard_dataset.local_path).resolve()

    assert str(local_path) == str(
        file_paths["yaml_fname_06_27"]
    ), "The OCD ARD path has not been updated"

    # Check that trying to move a dir that is already moved
    # doesn't cause an error
    blocked_scenes = [
        {
            "blocking_ard_id": get_blocked_scene_ard(),
            "blocked_l1_zip_path": "not used",
            "blocking_ard_path": file_paths["yaml_fname_06_27"],
        }
    ]

    l1_zips, uuids2archive = move_blocked(
        blocked_scenes,
        file_paths["current_base_path"].resolve(),
        file_paths["new_base_path"].resolve(),
    )

    # Assert the dir ... is still there
    assert os.path.isfile(
        file_paths["yaml_fname_06_27"]
    ), "The yaml file should still persist but it is not"
    # There is 1 l1, so it will be reprocessed
    assert (
        len(l1_zips) == 1
    ), "No reprocessing will occur because there is no level 1 data"
    # The blocking ard will be archived
    assert (
        len(uuids2archive) == 1
    ), "No archival occuring because no blocking ard is detected"
