#!/usr/bin/env python3

from pathlib import Path
from click.testing import CliRunner
import os.path
import uuid
from subprocess import check_output, STDOUT
import pytest
import os
import sys
import tempfile
from typing import Optional, Tuple, Dict

from datacube import Datacube
from datacube.index import Index
from datacube.model import DatasetType, Range
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents
from scene_select.ard_reprocessed_l1s import (
    ard_reprocessed_l1s,
    DIR_TEMPLATE,
    move_blocked,
    find_blocked,
)
from scene_select.do_ard import ARCHIVE_FILE, ODC_FILTERED_FILE, PBS_ARD_FILE

# I had to do this to point to the
# test DB.
if True:
    user_id = os.environ["USER"]
    os.environ[
        "ODC_TEST_DB_URL"
    ] = f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"


def get_directory(data_type: str) -> Path:
    """
    this is the same as the following from
    db_index.sh
        SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
        TEST_DATA_REL="${SCRIPT_DIR}/../test_data/ls9_reprocessing"
        TEST_DATA=$(realpath "$TEST_DATA_REL")
    """
    # Get the absolute path of the script directory
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    #    if data_type.lower() == "data":
    #        # Define the relative path to the test data directory from SCRIPT_DIR
    #        TEST_DATA_REL = "../test_data/data"

    if data_type.lower() == "metadata":
        # Define the relative path to the test data directory from SCRIPT_DIR
        TEST_DATA_REL = "../data/metadata"

    elif data_type.lower() == "product":
        # Define the relative path to the test data directory from SCRIPT_DIR
        TEST_DATA_REL = "../data/product"

    elif data_type.lower() == "reprocessing":
        # Define the relative path to the test data directory from SCRIPT_DIR
        TEST_DATA_REL = "../test_data/ls9_reprocessing"

    else:
        return None

    # Get the absolute path of the test data directory
    return Path(os.path.realpath(os.path.join(SCRIPT_DIR, TEST_DATA_REL)))


metadata_directory = get_directory("metadata")
METADATA_TYPES = [
    os.path.join(metadata_directory, "eo3_landsat_l1.odc-type.yaml"),
    os.path.join(metadata_directory, "eo3_landsat_ard.odc-type.yaml"),
]

product_directory = get_directory("product")
PRODUCTS = [
    os.path.join(product_directory, "l1_ls9.odc-product.yaml"),
    os.path.join(product_directory, "ga_ls9c_ard_3.odc-product.yaml"),
]
DATASETS = []

pytestmark = pytest.mark.usefixtures("setup_all_fixtures")


@pytest.fixture
def setup_config_file():
    """
    Create the temporary config file so that ard scene select
    processes that get runned in this script as subprocesses
    will be able to access the same datacube instance"""

    user_id = os.getenv("USER")  # Fetch the current user ID
    yaml_content = f"""
[{user_id}_automated_testing]

db_port: 6432
db_database: {user_id}_automated_testing
"""
    # Non- GADI runs - TODO - either remove this or detect non-gadi runs
    # observe the default port for pg is 532 but gadi uses 6432
    #    yaml_content = f"""
    # [datacube]
    # db_hostname: localhost
    # db_port: 5432
    # db_database: gy5636_automated_testing
    # """

    print(f"The yaml config content is {yaml_content}")
    temp_file_path = None
    try:
        # Create the temporary file without delete=False
        temp_file = tempfile.NamedTemporaryFile(mode="w", delete="False")
        temp_file_path = temp_file.name

        # Write the YAML content to the file
        temp_file.write(yaml_content)
        temp_file.flush()

        # Return the path to the temporary file
        yield temp_file_path
    finally:
        # Close the file explicitly (since delete=False was not used)
        if temp_file_path is not None:
            temp_file.close()


def add_dataset(odc_test_db, filename):
    create_dataset = Doc2Dataset(odc_test_db.index)
    for _, doc in read_documents(filename):
        dataset, err = create_dataset(
            doc,
            Path(filename).absolute().as_uri(),
        )

        assert dataset is not None, err
        created = odc_test_db.index.datasets.add(dataset)
        assert created.uris


@pytest.fixture
def setup_environment_variables(setup_config_file):
    user_id = os.environ["USER"]
    os.environ[
        "ODC_TEST_DB_URL"
    ] = f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"

    # Set environment variables for the test
    # Set the DATACUBE_ENVIRONMENT and DATACUBE_CONFIG_PATH
    os.environ["DATACUBE_CONFIG_PATH"] = setup_config_file
    user_id = os.environ["USER"]
    os.environ["DATACUBE_ENVIRONMENT"] = f"{user_id}_automated_testing"    

    yield  # Nothing to return, but the setup is done before running the test
    del os.environ["DATACUBE_CONFIG_PATH"]
    del os.environ["DATACUBE_ENVIRONMENT"]
    del os.environ["ODC_TEST_DB_URL"]


@pytest.fixture
def setup_local_directories_and_files():
    setup_script = Path(__file__).parent.joinpath("setup_file_paths.sh")

    cmd = [setup_script]
    try:
        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
        print("====================")
        print(cmd_stdout)
        print("====================")
    except Exception as e:
        print(e.output.decode())  # print out the stdout messages up to the exception
        print(e)  # To print out the exception message

    yield Path(__file__).parent.joinpath("scratch")


@pytest.fixture
def setup_and_test_datacube_scenarios(odc_test_db: Datacube):
    """
    Introduce blocking by archiving
    """
    # ---------------------
    # Add two ARDs that are blocking two l1s
    # ---------------------
    reprocessing_directory = get_directory("reprocessing")

    # ---------------------
    # Add two ARDs that are blocking two l1s
    # ---------------------
    # add and archive the l1 that produces the blocking ARD
    # 4c68b81a-23a0-5e57-b983-96439fc4518c
    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml",
        ),
    )

    ids_to_archive = [
        "4c68b81a-23a0-5e57-b983-96439fc4518c",
        "d530018e-5dad-58c2-8471-15f17d506604",
    ]

    odc_test_db.index.datasets.archive([ids_to_archive[0]])

    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
        ),
    )

    # Add the l1 that is blocked by the ARD
    # blocked l1 91e7489e-f05a-5b7e-a96c-f0f0549bdd34
    add_dataset(
        odc_test_db,
        "/g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml",  # GRR
    )

    # ---------------------
    # add and archive the l1 that produces the blocking ARD
    # level1: d530018e-5dad-58c2-8471-15f17d506604
    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml",
        ),
    )

    odc_test_db.index.datasets.archive([ids_to_archive[1]])

    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
        ),
    )

    add_dataset(
        odc_test_db,
        "/g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml",  # GRR
    )

    # Add a non-blocking ARD
    # add the l1 level1:  a230aceb-528b-5895-a4d7-94226e172dcf
    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "l1_Landsat_C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml",
        ),
    )

    # Add the non-blocking ARD
    # 43b726eb-77bd-42ac-bd11-ad1eea11863e
    add_dataset(
        odc_test_db,
        os.path.join(
            reprocessing_directory,
            "a_ga_ls9c_ard_3_raw/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml",
        ),
    )

    for tmp_dataset in odc_test_db.index.datasets.search_returning(
        ("id",), product="ga_ls9c_ard_3"
    ):
        ard_id = tmp_dataset.id
        ard_dataset = odc_test_db.index.datasets.get(ard_id, include_sources=True)
        l1_id = ard_dataset.metadata_doc["lineage"]["source_datasets"]["level1"]["id"]
        l1_ds = odc_test_db.index.datasets.get(l1_id)

    # We need to make sure we can find the blocked entries else ard
    # reprocessing will not work
    results = find_blocked(odc_test_db, "ga_ls9c_ard_3", 2)

    assert results is not None, f"Cannot find any blocked datasets: {results}"

    yield odc_test_db  # Yield the datacube instance for use in the tests

    odc_test_db.index.datasets.restore(ids_to_archive)


# Make setup_and_test_datacube_scenarios an automatic
# fixture by using autouse=True
@pytest.fixture(autouse=True)
def setup_all_fixtures(
    setup_config_file,
    setup_environment_variables,
    setup_local_directories_and_files,
    auto_odc_db,
    setup_and_test_datacube_scenarios,
):
    odc_db = setup_and_test_datacube_scenarios
    dynamic_config_file = setup_config_file
    # Yield both odc_db and dynamic_config_file as a tuple
    yield odc_db, dynamic_config_file


def test_datacube_requirements(setup_config_file):
    """
    Ensure the datacube is ready to go
    """

    # this is how we access a fixture value IF we don't force it to run first
    # at the test function definition level
    temp_file_name = setup_config_file

    assert os.path.isfile(temp_file_name), "Config file does not exist"
    assert (
        os.environ["DATACUBE_CONFIG_PATH"] == setup_config_file
    ), "Config file is not the one we expect"


    user_id = os.environ["USER"]
    assert (
        os.environ["DATACUBE_ENVIRONMENT"] == f"{user_id}_automated_testing"    
    ), "Datacube environment is wrong"

    user_id = os.getenv("USER")
    expected_url = (
        f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"
    )
    assert (
        os.environ["ODC_TEST_DB_URL"] == expected_url
    ), f"ODC_TEST_DB_URL env variable not set to {expected_url}"


def test_is_dc_ready(setup_all_fixtures: Tuple[Datacube, str]):

    odc_db, _ = setup_all_fixtures
    my_dc = odc_db.find_datasets(product="usgs_ls9c_level1_2")

    # Check if the dataset list is not empty (i.e., dataset exists)
    assert my_dc, "Dataset not found. Test failed."
    assert my_dc is not None, f"DC retrieval test-{my_dc}, "


def get_file_paths_for_test_ard_reprocessed_l1s() -> Dict:
    REPROCESS_TEST_DIR = get_directory("reprocessing")

    return {
        "reprocess_test_dir": get_directory("reprocessing"),
        "scratch_dir": Path(__file__).parent.joinpath("scratch"),
        "current_base_path": REPROCESS_TEST_DIR,
        "new_base_path": REPROCESS_TEST_DIR.joinpath("moved"),
        "old_dir_06_27": REPROCESS_TEST_DIR.joinpath(
            "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
        ),
        "old_yaml_fname_06_27": REPROCESS_TEST_DIR.joinpath(
            "ga_ls9c_ard_3",
            "102",
            "076",
            "2022",
            "06",
            "27",
            "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
        ),
        "new_dir_06_21": REPROCESS_TEST_DIR.joinpath(
            "moved", "ga_ls9c_ard_3", "092", "081", "2022", "06", "21"
        ),
        "fname_06_21": REPROCESS_TEST_DIR.joinpath(
            "moved",
            "ga_ls9c_ard_3",
            "092",
            "081",
            "2022",
            "06",
            "21",
            "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
        ),
        "new_dir_06_27": REPROCESS_TEST_DIR.joinpath(
            "moved", "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
        ),
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


def test_ard_reprocessed_l1s(setup_all_fixtures: Tuple[Datacube, str]):
    """Test the ard_reprocessed_l1s function."""

    dry_run = False
    product = "ga_ls9c_ard_3"

    scene_limit = 2
    run_ard = False

    odc_db, dynamic_config_file = setup_all_fixtures
    temp_file_name = dynamic_config_file

    assert os.path.isfile(temp_file_name), "Config file does not exist"
    file_paths = get_file_paths_for_test_ard_reprocessed_l1s()
    logdir = file_paths["scratch_dir"]

    jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    cmd_params = [
        "--current-base-path",
        file_paths["current_base_path"],
        "--new-base-path",
        file_paths["new_base_path"],
        "--product",
        product,
        "--scene-limit",
        scene_limit,
        "--logdir",
        file_paths["scratch_dir"],  # SCRATCH_DIR,
        "--jobdir",
        str(jobdir),
        "--workdir",
        file_paths["scratch_dir"],  # SCRATCH_DIR,
    ]
    if run_ard:
        cmd_params.append("--run-ard")
    if dry_run:
        cmd_params.append("--dry-run")

    runner = CliRunner()
    # Set up the environment variables before running the cli command
    env_vars = {
        "ODC_TEST_DB_URL": os.environ["ODC_TEST_DB_URL"],
        "DATACUBE_CONFIG_PATH": os.environ["DATACUBE_CONFIG_PATH"],
        "DATACUBE_ENVIRONMENT": os.environ["DATACUBE_ENVIRONMENT"],
    }

    print("Environment Variables for ard_reprocessed_l1s:")
    print(f"\tODC_TEST_DB_URL: {env_vars['ODC_TEST_DB_URL']}")
    print(f"\tDATACUBE_CONFIG_PATH: {env_vars['DATACUBE_CONFIG_PATH']}")
    print(f"\tDATACUBE_ENVIRONMENT: {env_vars['DATACUBE_ENVIRONMENT']}")
    assert os.path.isfile(
        env_vars["DATACUBE_CONFIG_PATH"]
    ), "Config file does not exist"

    result = runner.invoke(ard_reprocessed_l1s, cmd_params, env=env_vars)
    print("***** results output ******")
    print(result.output)
    print("***** results exception ******")
    print(result.exception)
    print("***** results end ******")

    # Assert a few things
    # Two dirs have been moved.  These are the previous datasets
    # that we sent in for reprocessing.
    assert os.path.isfile(
        # fname_06_21
        file_paths["fname_06_21"],
    ), f"The yaml file, '{file_paths['fname_06_21']}' has been moved, for a different scene"

    assert os.path.isfile(
        # yaml_fname_06_27
        file_paths["yaml_fname_06_27"],
    ), f"The yaml file, '{file_paths['yaml_fname_06_27']}' has been moved"

    ard_dataset = odc_db.index.datasets.get(get_blocked_scene_ard())
    local_path = Path(ard_dataset.local_path).resolve()
    assert str(local_path) == str(
        file_paths["yaml_fname_06_27"]
    ), "The OCD ARD path has been updated"

    # uuids have been written to an archive file
    filename = jobdir.joinpath(ARCHIVE_FILE)
    with open(filename, "r", encoding="utf-8") as f:
        temp = f.read().splitlines()

    assert sorted(
        ["3de6cb49-60da-4160-802b-65903dcbbac8", "d9a499d1-1abd-4ed1-8411-d584ca45de25"]
    ) == sorted(temp), "The correct uuids have been written to the archive file"

    filename = jobdir.joinpath(ODC_FILTERED_FILE)
    with open(filename, "r", encoding="utf-8") as f:
        temp = f.read().splitlines()

    if "HOSTNAME" in os.environ and "gadi" in os.environ["HOSTNAME"]:
        base_location = Path("/g/data/da82/AODH/USGS/L1/Landsat/C2/")
    else:
        base_location = file_paths["reprocess_test_dir"].joinpath("l1_Landsat_C2")
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
    assert sorted([str(a_l1_tar), str(b_l1_tar)]) == sorted(
        temp
    ), "The correct l1 tars have been written to the scene select file"

    filename = jobdir.joinpath(PBS_ARD_FILE)
    assert os.path.isfile(filename), "There is a run ard pbs file"


def get_blocked_scene_ard() -> str:
    # ard_id_06_27
    return "d9a499d1-1abd-4ed1-8411-d584ca45de25"


def get_file_paths_for_test_move_blocked() -> Dict:
    REPROCESS_TEST_DIR = get_directory("reprocessing")

    return {
        "reprocess_test_dir": get_directory("reprocessing"),
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
        "scratch_dir": Path(__file__).parent.joinpath("scratch"),
    }


def test_move_blocked(setup_all_fixtures: Tuple[Datacube, str]):
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
        file_paths["current_base_path"],  # .resolve(),
        file_paths["new_base_path"],  # .resolve(),
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

    odc_db, _ = setup_all_fixtures
    ard_dataset = odc_db.index.datasets.get(get_blocked_scene_ard())
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
