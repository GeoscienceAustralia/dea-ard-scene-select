#!/usr/bin/env python3

from scene_select.ard_reprocessed_l1s import ard_reprocessed_l1s

from pathlib import Path

def test_scene_move():
    # the current definition of current_path
    # Update this to just be the directory
    config = None
    current_path = Path("/g/data/u46/users/dsg547/test_data/c3/ls9_reprocessing/ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml")
    current_base_path = Path("/g/data/u46/users/dsg547/test_data/c3/ls9_reprocessing/")
    new_base_path = Path("/g/data/u46/users/dsg547/test_data/c3/ls9_reprocessing/moved")
    dry_run = True
    product = "ga_ls9c_ard_3"
    logdir = Path(".")
    stop_logging = False
    log_config = None
    scene_limit = 1
    run_ard = False

    nothing_yet = ard_reprocessed_l1s.__wrapped__(
    config,
    current_base_path,
    new_base_path,
    product,
    logdir,
    stop_logging,
    log_config,
    scene_limit,
    run_ard,
    )

