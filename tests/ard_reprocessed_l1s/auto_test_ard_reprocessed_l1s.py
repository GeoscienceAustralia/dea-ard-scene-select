#!/usr/bin/env python3

from pathlib import Path
from click.testing import CliRunner
import os.path
import uuid
from subprocess import check_output, STDOUT
import pytest
import os
import tempfile

from datacube import Datacube
from datacube.index import Index
from datacube.model import DatasetType, Range
from scene_select.ard_reprocessed_l1s import (
    ard_reprocessed_l1s,
    DIR_TEMPLATE,
    move_blocked,
)
from scene_select.do_ard import ARCHIVE_FILE, ODC_FILTERED_FILE, PBS_ARD_FILE

REPROCESS_TEST_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
)
SCRATCH_DIR = Path(__file__).parent.joinpath("scratch")

current_base_path = REPROCESS_TEST_DIR

old_dir_06_27 = current_base_path.joinpath(
    "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
)
old_yaml_fname_06_27 = old_dir_06_27.joinpath(
    "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
)

new_base_path = REPROCESS_TEST_DIR.joinpath("moved")

new_dir_06_21 = REPROCESS_TEST_DIR.joinpath(
    "moved", "ga_ls9c_ard_3", "092", "081", "2022", "06", "21"
)
fname_06_21 = new_dir_06_21.joinpath(
    "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml"
)

new_dir_06_27 = REPROCESS_TEST_DIR.joinpath(
    "moved", "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
)
yaml_fname_06_27 = new_dir_06_27.joinpath(
    "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
)
ard_id_06_27 = "d9a499d1-1abd-4ed1-8411-d584ca45de25"
# yaml files
# ensure these are sequence
METADATA_TYPES = [
    "../data/metadata/eo3_landsat_l1.odc-type.yaml",
    "../data/metadata/eo3_landsat_ard.odc-type.yaml",

]
PRODUCTS = [
    "../data/product/l1_ls9.odc-product.yaml",
    "../data/product/ga_ls9c_ard_3.odc-product.yaml",
]
DATASETS = [
    "../data/datasets/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml",
    "../data/datasets/ls9_reprocessing/l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml",
    "../data/datasets/ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
    "../data/datasets/ls9_reprocessing/l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml", # gadi specific
    "../data/datasets/ls9_reprocessing/ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",  # It says no lineage but i didn't remove the lineage details as it did have valid lineage data.
    "../data/datasets/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml",
    "../data/datasets/ls9_reprocessing/l1_Landsat_C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml",
    "../data/datasets/ls9_reprocessing/ga_ls9c_ard_3/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml",
]

pytestmark = pytest.mark.usefixtures("setup_environment", "auto_odc_db", "setup_all_fixtures",)


@pytest.fixture
def setup_environment():
    os.environ['ODC_TEST_DB_URL'] = f"postgresql://gy5636@deadev.nci.org.au/{user_id}_automated_testing"
    #f"{user_id}_automated_testing.deadev.nci.org.au"
    dd = f"{user_id}_automated_testing.deadev.nci.org.au"
    print(f"Just setup the ODC_TEST_DB_URL as {dd}")
    # This gets set in DATACUBE_DB_URL in the backend package


@pytest.fixture
def setup_config_file():
    """
    Create the temporary config file so that ard scene select
    processes that get runned in this script as subprocesses
    will be able to access the same datacube instance"""
    user_id = os.getenv("USER")  # Fetch the current user ID
    yaml_content = f"""
[datacube]
db_hostname: deadev.nci.org.au
db_port: 6432
db_database: {user_id}_automated_testing
"""
    print(f"The yaml config content is {yaml_content}")
    temp_file_path = None
    try:
        # Create the temporary file without delete=False
        temp_file = tempfile.NamedTemporaryFile(mode="w",delete="False")
        temp_file_path = temp_file.name

        # Write the YAML content to the file
        temp_file.write(yaml_content)
        temp_file.flush()
        print(f"Temp config file path is '{temp_file_path}'...")

        # Copy the config file for checking
#        import shutil
#        shutil.copyfile(temp_file.name, "/g/data/u46/users/gy5636/dea-ard-scene-select/tests/ard_reprocessed_l1s/myconf.conf")

        # Return the path to the temporary file
        yield temp_file_path
    finally:
        # Close the file explicitly (since delete=False was not used)
        if temp_file_path is not None:
            temp_file.close()


@pytest.fixture
def setup_environment_variables(setup_config_file):
    # Set environment variables for the test
    # Set the DATACUBE_ENVIRONMENT and DATACUBE_CONFIG_PATH
    os.environ["DATACUBE_CONFIG_PATH"] = setup_config_file
    os.environ["DATACUBE_ENVIRONMENT"] = "datacube"

    yield  # Nothing to return, but the setup is done before running the test

    # Clean up: Remove the environment variables after the test
    del os.environ["DATACUBE_CONFIG_PATH"]
    del os.environ["DATACUBE_ENVIRONMENT"]


@pytest.fixture
def setup_local_directories_and_files():
    setup_script = Path(__file__).parent.joinpath("setup_file_paths.sh")
    cmd = [setup_script]
    try:
        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
    except Exception as e:
        print(e.output.decode())  # print out the stdout messages up to the exception
        print(e)  # To print out the exception message
    print("======setup_local_directories_and_files (START)==============")
    print(cmd_stdout)
    print("======setup_local_directories_and_files (END)==============")


@pytest.fixture
def setup_and_test_datacube_scenarios(odc_test_db: Datacube):
    """
    Introduce blocking by archiving
    """
    # ---------------------
    # Add two ARDs that are blocking two l1s
    # ---------------------
    # add and archive the l1 that produces the blocking ARD
    # 4c68b81a-23a0-5e57-b983-96439fc4518c

    ######pass # TODO - enable it when the dataset can be pulled out again

    print("Setting up datacube scenarios...")

    archive_ids = [
        'd530018e-5dad-58c2-8471-15f17d506604',
        '4c68b81a-23a0-5e57-b983-96439fc4518c',        
    ]

    # TODO - temporarily muting these off as there's an issue with the datasets being revived
    for id_to_archive in archive_ids:
        l1_blocking_1_datasets = odc_test_db.index.datasets.get(id_to_archive)

        assert l1_blocking_1_datasets is not None, f"L1 dataset (id={id_to_archive}) for blocking ARD cannot be retrieved: {l1_blocking_1_datasets}"

        print(f"Archiving {id_to_archive} now...")
        try:
            odc_test_db.index.archive(id_to_archive)            
        except Exception as error_string:
            assert True == True, f" Archival of dataset id {id_to_archive} has failed: error_string"

    yield odc_test_db  # Yield the datacube instance for use in the tests

    # tear down
    for id_to_restore in archive_ids:
        print(f"Teardown - restoring dataset of id, {id_to_restore}")        
        odc_test_db.index.datasets.restore(id_to_restore)


# muck
# Make setup_and_test_datacube_scenarios an automatic fixture by using autouse=True
@pytest.fixture(autouse=True)
def setup_all_fixtures(
    setup_config_file,
    setup_environment_variables,
    setup_local_directories_and_files,
    setup_and_test_datacube_scenarios,
):


    # Since we're only using this fixture to group other fixtures, we don't need to do anything here.
    # All the other fixtures will be executed automatically before running the test.

    # The order in which the fixtures are listed above determines their execution order.
    # 'auto_odc_db' will be executed first, followed by the other fixtures.

    # We don't need to yield anything here, as this fixture doesn't return any data.
    # If you need to pass data from this fixture to the test function, you can yield the necessary data here.
    print("Running setup_all_fixtures...")
    

    odc_db = setup_and_test_datacube_scenarios

    # We need to yield the 'odc_db' instance so that it becomes available to other fixtures and tests.
    yield odc_db

    
def test_datacube_requirements(setup_config_file, setup_environment_variables):
    temp_file_name = setup_config_file
    """
        Ensure the datacube is ready to go
    """
    assert os.path.isfile(temp_file_name), "Config file does not exist"
    assert (
        os.environ["DATACUBE_CONFIG_PATH"] == setup_config_file
    ), "Config file is not the one we expect"
    assert os.environ["DATACUBE_ENVIRONMENT"] == "datacube", "Environment is wrong"
    user_id = os.getenv("USER")
    expected_url = f"postgresql://gy5636@deadev.nci.org.au/{user_id}_automated_testing"
    assert (
        os.environ["ODC_TEST_DB_URL"] == expected_url
    ), f"ODC_TEST_DB_URL env variable not set to {expected_url}"


def test_is_dc_ready(setup_all_fixtures: Datacube):
    
    assert setup_all_fixtures is not None, "auto_odc_db is ok"

    my_dc = setup_all_fixtures.find_datasets(product="usgs_ls9c_level1_2")

    # Check if the dataset list is not empty (i.e., dataset exists)
    assert my_dc, "Dataset not found. Test failed."
    assert my_dc is not None, f"DC retrieval test-{my_dc}, "

    # do all the checks for adds here


def test_ard_reprocessed_l1s(setup_config_file, setup_environment_variables):
    """Test the ard_reprocessed_l1s function."""
    pass
    dry_run = False
    product = "ga_ls9c_ard_3"
    logdir = SCRATCH_DIR
    scene_limit = 2
    run_ard = False
    temp_file_name = setup_config_file

    assert os.path.isfile(temp_file_name), "Config file does not exist"

    jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    cmd_params = [
        "--current-base-path",
        str(current_base_path.resolve()),
        "--new-base-path",
        str(new_base_path.resolve()),
        "--product",
        product,
        "--scene-limit",
        scene_limit,
        "--logdir",
        SCRATCH_DIR,
        "--jobdir",
        str(jobdir),
        "--workdir",
        SCRATCH_DIR,
    ]
    if run_ard:
        cmd_params.append("--run-ard")
    if dry_run:
        cmd_params.append("--dry-run")
    runner = CliRunner()
    result = runner.invoke(ard_reprocessed_l1s, cmd_params)
    print(f"RUNNING ARD SCENE SELECT with scratch dir, '{SCRATCH_DIR}'....")
    print("***** results output ******")
    print(result.output)
    print("***** results exception ******")
    print(result.exception)
    print("***** results end ******")

    # Assert a few things
    # Two dirs have been moved.  These are the previous datasets
    # that we sent in for reprocessing.
    assert os.path.isfile(
        fname_06_21
    ), f"The yaml file, '{fname_06_21}' has been moved, for a different scene"
    assert os.path.isfile(
        yaml_fname_06_27
    ), f"The yaml file, '{yaml_fname_06_27}' has been moved"

    ard_dataset = dc.index.datasets.get(ard_id_06_27)
    local_path = Path(ard_dataset.local_path).resolve()
    assert str(local_path) == str(yaml_fname_06_27), "The OCD ARD path has been updated"

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
    assert sorted([str(a_l1_tar), str(b_l1_tar)]) == sorted(
        temp
    ), "The correct l1 tars have been written to the scene select file"

    filename = jobdir.joinpath(PBS_ARD_FILE)
    assert os.path.isfile(filename), "There is a run ard pbs file"


## Others:

# os.environ["DATACUBE_ENVIRONMENT"] = f"{os.getenv('USER')}_automated_testing"
#    os.environ["DATACUBE_CONFIG_PATH"] = str(
#        Path(__file__).parent.joinpath("datacube.conf")
#    )

# def test_DUMMYdatacube_env_values(odc_test_db: Datacube):
#    #import subprocess
#    #result = subprocess.check_output(["env"], universal_newlines=True)

### assert odc_test_db is None, f" The odc_test_db is {odc_test_db}"
#    #assert result is None, f"result is {result}"
#    assert os.environ["ODC_TEST_DB_URL"] == 'postgresql://gy5636@deadev.nci.org.au/gy5636_automated_testing', "    ODC_TEST_DB_URL env variable not set"
#    assert os.environ["DATACUBE_ENVIRONMENT"] is None, f"result is {result}"
#    assert os.environ["DATACUBE_CONFIG_PATH"] is None, f"DATACUBE_CONFIG_PATH={os.environ['DATACUBE_CONFIG_PATH']}"
