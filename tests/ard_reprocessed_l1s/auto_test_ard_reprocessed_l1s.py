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

pytestmark = pytest.mark.usefixtures("auto_odc_db")


@pytest.fixture(autouse=True)
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

    ids_to_archive = [
        'd530018e-5dad-58c2-8471-15f17d506604',
        '4c68b81a-23a0-5e57-b983-96439fc4518c',        
    ]

    odc_test_db.index.datasets.archive(ids_to_archive)            

    yield odc_test_db  # Yield the datacube instance for use in the tests

    odc_test_db.index.datasets.restore(ids_to_archive)



def test_is_dc_ready(odc_test_db: Datacube):
    
    assert odc_test_db is not None, "odc_test_db is not ok"

    datasets = odc_test_db.find_datasets(product="usgs_ls9c_level1_2")

    # Check if the dataset list is not empty (i.e., dataset exists)
    assert datasets, "Dataset not found. Test failed."
    assert datasets is not None, f"DC retrieval test-{datasets}, "

    # do all the checks for adds here

@pytest.fixture()
def setup_ard_reprocessed_l1s(tmp_path):

    scratch_dir = Path(__file__).parent.joinpath("scratch")

    current_base_path = Path(__file__).parent.parent.joinpath("test_data/ls9_reprocessing").resolve()

    old_dir_06_27 = current_base_path.joinpath(
        "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
    )
    old_yaml_fname_06_27 = old_dir_06_27.joinpath(
        "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
    )

    new_base_path = tmp_path / "moved"
    import shutil
    shutil.copytree(current_base_path, new_base_path)


    return dict(current_base_path=current_base_path, 
                scratch_dir=scratch_dir,
                new_base_path=new_base_path,)

def test_ard_reprocessed_l1s(odc_db, setup_ard_reprocessed_l1s):
    """Test the ard_reprocessed_l1s function."""
    dry_run = False
    product = "ga_ls9c_ard_3"
    logdir = setup_ard_reprocessed_l1s['scratch_dir']
    current_base_path = setup_ard_reprocessed_l1s['current_base_path']
    new_base_path = setup_ard_reprocessed_l1s['new_base_path']

    new_dir_06_21 = new_base_path.joinpath( "ga_ls9c_ard_3/092/081/2022/06/21")
    fname_06_21 = new_dir_06_21.joinpath( "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml")

    new_dir_06_27 = new_base_path.joinpath( "ga_ls9c_ard_3/102/076/2022/06/27")
    yaml_fname_06_27 = new_dir_06_27.joinpath( "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml")
    ard_id_06_27 = "d9a499d1-1abd-4ed1-8411-d584ca45de25"

    scene_limit = 2
    run_ard = False

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
        logdir,
        "--jobdir",
        str(jobdir),
        "--workdir",
        logdir,
    ]
    if run_ard:
        cmd_params.append("--run-ard")
    if dry_run:
        cmd_params.append("--dry-run")
    runner = CliRunner()
    result = runner.invoke(ard_reprocessed_l1s, cmd_params)
    print(f"RUNNING ARD SCENE SELECT with scratch dir, '{logdir}'....")
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

    ard_dataset = odc_db.index.datasets.get(ard_id_06_27)
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

    base_location = current_base_path.joinpath("l1_Landsat_C2")

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
