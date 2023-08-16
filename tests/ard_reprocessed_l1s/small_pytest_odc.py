#!/usr/bin/env python3

"""
Testing connecting to a ODC database using
pytest-odc

This should currently pass, with logs showing ard_reprocessed_l1s
is using the test DB.

What it is missing
All the scenes that are used in the master test
Checking of the results in test_ard_reprocessed_l1s
Constants used in testing the results
test_move_blocked

Recomendations
Use constants, not functions for holding values
used to test the results. for test_ard_reprocessed_l1s
what is used in master
should just work. Though they could be improved by
writing all the names in capitals. Plus changing
the names to be more descriptive / adding comments.

Refactor the fixture setup_local_directories_and_files
to use python.

Notes
The metadata and product definitions have been copied into a
single dir, in the test_data dir.

Setting the ODC DB has been moved into the calling bash script. This
module can just assume there will be a DB.
There are a few 'if False' sections of code taht I used
to work out what was happening. If it is all working smoothly
they can be removed.

I haven't checked if using pytst-odc to set the location breaks
 things. It looked ok in the logs though.
"""

from pathlib import Path
import os
import pytest
from click.testing import CliRunner
import os.path
import uuid
from subprocess import check_output, STDOUT

import datacube
from scene_select.ard_reprocessed_l1s import (
    ard_reprocessed_l1s,
    DIR_TEMPLATE,
    move_blocked,
)
from scene_select.do_ard import ARCHIVE_FILE, ODC_FILTERED_FILE, PBS_ARD_FILE


if False:
    user_id = os.environ["USER"]
    os.environ[
        "ODC_TEST_DB_URL"
    ] = f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ODC_YAML_DIR = Path(__file__).parent.joinpath("..", "test_data", "odc_setup").resolve()
SCENES_DIR = Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()

METADATA_TYPES = [ODC_YAML_DIR / "eo3_landsat_l1.odc-type.yaml",
                  ODC_YAML_DIR / "eo3_landsat_ard.odc-type.yaml",]
PRODUCTS = [ODC_YAML_DIR / "l1_ls9.odc-product.yaml",
            ODC_YAML_DIR / "ga_ls9c_ard_3.odc-product.yaml",]

# two l1 scenes and one ard scene
# the first l1 to be archived in the fixture
group1 =    [
    SCENES_DIR / "l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml",
    SCENES_DIR / "l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml",
    SCENES_DIR / "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml"
    ]

DATASETS = group1

def setup_local_directories_and_files():
    setup_script = Path(__file__).parent.joinpath("setup_file_paths.sh")

    cmd = [setup_script]
    cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()

# Have a clean directory structure
setup_local_directories_and_files()


# This needs to be here
pytestmark = pytest.mark.usefixtures("auto_odc_db")

if False:
    # For pytest-odc the ODC DB location is
    # set using ODC_TEST_DB_URL
    # This crashes trying to use docker.
    # Set environment variables for the test
    # Set the DATACUBE_ENVIRONMENT and DATACUBE_CONFIG_PATH
    #os.environ["DATACUBE_CONFIG_PATH"] = str(Path(__file__).parent.joinpath("datacube.conf"))
    user_id = os.environ["USER"]
    os.environ["DATACUBE_ENVIRONMENT"] = f"{user_id}_automated_testing"    




@pytest.fixture
def archive(odc_test_db):
    group1_id_2_archive = "4c68b81a-23a0-5e57-b983-96439fc4518c"
    odc_test_db.index.datasets.archive([group1_id_2_archive])


def test_ard_reprocessed_l1s(archive):
    """Test the ard_reprocessed_l1s function."""

    if False:
        # unsetting the ODC_TEST_DB_URL environment variable
        # Doing this does not impact running ard_reprocessed_l1s
        os.environ[
            "ODC_TEST_DB_URL"
        ] = f"postgresql://{user_id}@deadev.nci.org.au/XXXXXXXX"

    if False:
        # unsetting the DATACUBE_DB_URL environment variable
        # This stops ard_reprocessed_l1s from running
        os.environ[
            "DATACUBE_DB_URL"
        ] = f"postgresql://{user_id}@deadev.nci.org.au/XXXXXXXX"

    REPROCESS_TEST_DIR = (
        Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
    )
    
    new_base_path = REPROCESS_TEST_DIR.joinpath("moved")
    current_base_path = REPROCESS_TEST_DIR
    SCRATCH_DIR = Path(__file__).parent.joinpath("scratch")
    dry_run = False
    product = "ga_ls9c_ard_3"
    logdir = SCRATCH_DIR
    scene_limit = 2
    run_ard = False

    # in bash
    # hex=$(openssl rand -hex 3)
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

    # pytset-odc has set
    # DATACUBE_DB_URL, based on the ODC_TEST_DB_URL
    # such that the correct ODC DB is used
    if run_ard: 
        cmd_params.append("--run-ard")
    if dry_run:
        cmd_params.append("--dry-run")
    runner = CliRunner()
    result = runner.invoke(ard_reprocessed_l1s, cmd_params)
    print("***** results output ******")
    print(result.output)
    print("***** results exception ******")
    print(result.exception)
    print("***** results end ******")
