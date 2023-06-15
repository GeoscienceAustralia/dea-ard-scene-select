#!/usr/bin/env python3

from pathlib import Path
from click.testing import CliRunner
import os.path
import uuid
from subprocess import check_output, STDOUT
import pytest
import os

import datacube
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


@pytest.fixture
def set_up_dirs_and_db():
    setup_script = Path(__file__).parent.joinpath("db_index.sh")
    cmd = [setup_script]
    try:
        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
    except Exception as e:
        print(e.output.decode())  # print out the stdout messages up to the exception
        print(e)  # To print out the exception message
    print("====================")
    print(cmd_stdout)
    print("====================")

    # So the scene select call uses the correct DB
    if "HOSTNAME" in os.environ and "gadi" in os.environ["HOSTNAME"]:
        # Nobody call their system Brigadiers, ok.
        end_tag = "_dev"
    else:
        end_tag = "_local"
    os.environ["DATACUBE_ENVIRONMENT"] = f"{os.getenv('USER')}{end_tag}"
    os.environ["DATACUBE_CONFIG_PATH"] = str(
        Path(__file__).parent.joinpath("datacube.conf")
    )


def test_ard_reprocessed_l1s(set_up_dirs_and_db):
    """Test the ard_reprocessed_l1s function."""

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
    assert os.path.isfile(yaml_fname_06_27), "The yaml file has been moved"
    assert os.path.isfile(
        fname_06_21
    ), "The yaml file has been moved, for a different scene"

    # Assert the ODC location info is correct
    dc = datacube.Datacube(
        app="test_ard_reprocessed_l1s", config=str(os.getenv("DATACUBE_CONFIG_PATH"))
    )
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
        "092_081", "LC90920812022172", "LC09_L1TP_092081_20220621_20220802_02_T1.tar",
    )
    b_l1_tar = base_location.joinpath(
        "102_076", "LC91020762022178", "LC09_L1TP_102076_20220627_20220802_02_T1.tar",
    )
    assert sorted([str(a_l1_tar), str(b_l1_tar)]) == sorted(
        temp
    ), "The correct l1 tars have been written to the scene select file"

    filename = jobdir.joinpath(PBS_ARD_FILE)
    assert os.path.isfile(filename), "There is a run ard pbs file"


def test_move_blocked(set_up_dirs_and_db):
    """ Test the move_blocked function."""

    # Test the move_blocked function
    # for a 'normal' case
    blocked_scenes = [
        {
            "blocking_ard_id": ard_id_06_27,
            "blocked_l1_zip_path": "not used",
            "blocking_ard_path": old_yaml_fname_06_27,
        }
    ]
    l1_zips, uuids2archive = move_blocked(
        blocked_scenes, current_base_path.resolve(), new_base_path.resolve()
    )

    # Test that in the case that
    # a move_blocked has occurred, but the
    # ARD is still blocking
    #  reprocessing will still occur
    # since the l1 scene is in the l1 zip list
    # Assert the dir has been moved
    assert os.path.isfile(yaml_fname_06_27), "The yaml file has been moved"
    assert len(l1_zips) == 1, "There should be one l1 zip"
    assert len(uuids2archive) == 1, "There should be one uuid to archive"
    dc = datacube.Datacube(
        app="test_ard_reprocessed_l1s", config=str(os.getenv("DATACUBE_CONFIG_PATH"))
    )
    ard_dataset = dc.index.datasets.get(ard_id_06_27)
    local_path = Path(ard_dataset.local_path).resolve()
    assert str(local_path) == str(yaml_fname_06_27), "The OCD ARD path has been updated"

    # Check that trying to move a dir that is already moved
    # doesn't cause an error
    blocked_scenes = [
        {
            "blocking_ard_id": ard_id_06_27,
            "blocked_l1_zip_path": "not used",
            "blocking_ard_path": yaml_fname_06_27,
        }
    ]
    l1_zips, uuids2archive = move_blocked(
        blocked_scenes, current_base_path.resolve(), new_base_path.resolve()
    )

    # Assert the dir ... is still there
    assert os.path.isfile(yaml_fname_06_27), "The yaml file is still there"
    assert len(l1_zips) == 1, "There is 1 l1, so it will be reprocessed"
    assert len(uuids2archive) == 1, "The blocking ard will be archived"
