#!/usr/bin/env python3

from pathlib import Path
from click.testing import CliRunner
import os.path
import uuid
from subprocess import check_output, STDOUT
import pytest
import os

from scene_select.ard_reprocessed_l1s import ard_reprocessed_l1s, DIR_TEMPLATE
from scene_select.do_ard import ARCHIVE_FILE, ODC_FILTERED_FILE, PBS_ARD_FILE

REPROCESS_TEST_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
)
SCRATCH_DIR = Path(__file__).parent.joinpath("scratch")

# orig_arl1s = ard_reprocessed_l1s.__wrapped__

@pytest.fixture
def set_up_dirs_and_db():
    setup_script = Path(__file__).parent.joinpath("db_index.sh")
    cmd = [setup_script]
    try:
        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
    except Exception as e:
        print(e.output.decode()) # print out the stdout messages up to the exception
        print(e) # To print out the exception message
    print ('====================')
    print (cmd_stdout)
    print ('====================')
    os.environ['DATACUBE_ENVIRONMENT'] = 'dsg547_dev_local'
    os.environ['DATACUBE_CONFIG_PATH'] = str(Path(__file__).parent.joinpath("datacube.conf"))


def test_scene_move(set_up_dirs_and_db):
    """Test the scene move function."""

    current_base_path = REPROCESS_TEST_DIR
    new_base_path = REPROCESS_TEST_DIR.joinpath("moved")
    dry_run = False
    product = "ga_ls9c_ard_3"
    logdir = SCRATCH_DIR
    scene_limit = 1
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

    # Assert a few things
    # Two dirs have been moved
    new_dir = REPROCESS_TEST_DIR.joinpath(
        "moved", "ga_ls9c_ard_3", "092", "081", "2022", "06", "21"
    )
    fname = new_dir.joinpath(
        "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml"
    )
    assert os.path.isfile(fname) == True

    new_dir = REPROCESS_TEST_DIR.joinpath(
        "moved", "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
    )
    fname = new_dir.joinpath(
        "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
    )
    assert os.path.isfile(fname) == True

    # uuids have been written to an archive file
    filename = jobdir.joinpath(ARCHIVE_FILE)
    temp = open(filename, "r").read().splitlines()
    assert sorted(
        ["3de6cb49-60da-4160-802b-65903dcbbac8", "d9a499d1-1abd-4ed1-8411-d584ca45de25"]
    ) == sorted(temp)
    # The l1's have been written to a zip file
    filename = jobdir.joinpath(ODC_FILTERED_FILE)
    temp = open(filename, "r").read().splitlines()
    a_dir = REPROCESS_TEST_DIR.joinpath(
        "l1_Landsat_C2",
        "092_081",
        "LC90920812022172",
        "LC09_L1TP_092081_20220621_20220802_02_T1.tar",
    )
    b_dir = REPROCESS_TEST_DIR.joinpath(
        "l1_Landsat_C2",
        "102_076",
        "LC91020762022178",
        "LC09_L1TP_102076_20220627_20220802_02_T1.tar",
    )
    assert sorted([str(a_dir), str(b_dir)]) == sorted(temp)
    # There is a run ard pbs file
    filename = jobdir.joinpath(PBS_ARD_FILE)
    assert os.path.isfile(fname) == True
