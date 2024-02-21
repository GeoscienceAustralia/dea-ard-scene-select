#!/usr/bin/env python3

import math
from typing import Optional

from scene_select.dass_logs import LOGGER
from pathlib import Path
import subprocess
import os
import stat

ODC_FILTERED_FILE = "scenes_to_ARD_process.txt"
ARCHIVE_FILE = "uuid_to_archive.txt"
PBS_ARD_FILE = "run_ard_pbs.sh"
PBS_JOB = """#!/bin/bash
module purge
module load pbs

source {env}

ard_pbs --level1-list {scene_list} {ard_args}
"""


def _calc_node_with_defaults(ard_click_params, count_all_scenes_list):
    # Estimate the number of nodes needed

    hours_per_granule = 7.5
    if ard_click_params["nodes"] is None:
        if ard_click_params["walltime"] is None:
            walltime = "10:00:00"
        else:
            walltime = ard_click_params["walltime"]
        if ard_click_params["workers"] is None:
            workers = 30
        else:
            workers = ard_click_params["workers"]
        ard_click_params["nodes"] = _calc_nodes_req(
            count_all_scenes_list, walltime, workers, hours_per_granule
        )
    hours, _, _ = (int(x) for x in walltime.split(":"))

    if hours <= hours_per_granule:
        raise ValueError("wall time <= hours per granule")


def _calc_nodes_req(granule_count, walltime, workers, hours_per_granule=7.5):
    """Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59', 28)
    2
    >>> _calc_nodes_req(800, '20:00', 28)
    3
    """
    hours, _, _ = (int(x) for x in walltime.split(":"))
    # to avoid divide by zero errors
    if hours == 0:
        hours = 1
    total_hours = float(hours_per_granule * granule_count)
    nodes = int(math.ceil(total_hours / (hours * workers)))
    if nodes == 0:
        # A zero node request to ard causes errors.
        nodes = 1
    return nodes


def dict2ard_arg_string(ard_click_params):
    ard_params = []
    for key, value in ard_click_params.items():
        if value is None:
            continue
        if key == "test":
            if value is True:
                ard_params.append("--" + key)
            continue
        if key == "yamls_dir":
            if value == "":
                # remove the yamls-dir if it is empty
                continue
        # convert underscores to dashes
        key = key.replace("_", "-")
        ard_params.append("--" + key)
        # Make path strings absolute
        if key in ("workdir", "logdir", "pkgdir", "index-datacube-env", "yamls-dir"):
            value = Path(value).resolve()
        ard_params.append(str(value))
    ard_arg_string = " ".join(ard_params)
    return ard_arg_string


def make_ard_pbs(level1_list, **ard_click_params):

    if ard_click_params["env"] is None:
        # Don't error out, just
        # fill env with a bad value
        env = "None"
    else:
        env = Path(ard_click_params["env"]).resolve()

    ard_args = dict2ard_arg_string(ard_click_params)
    pbs = PBS_JOB.format(env=env, scene_list=level1_list, ard_args=ard_args)
    return pbs


def do_ard(
    ard_click_params: dict,
    l1_count: int,
    usgs_level1_files: Optional[Path],
    uuids2archive: list,
    jobdir: Path,
    run_ard: bool,
    l1_zips=None,
):
    """Run ard.
    This function assumes a l1 zip file has been written to the jobdir.
    Though if you specify l1_zips in a list, and usgs_level1_files is None,
      it will write the file."""
    LOGGER.info("do_ard", **locals())
    try:
        _calc_node_with_defaults(ard_click_params, l1_count)
    except ValueError as err:
        print(err.args)
        LOGGER.warning("ValueError", message=err.args)

    if l1_zips is not None:
        if usgs_level1_files is not None:
            raise RuntimeError(f"Expected either l1_zips or usgs_level1_files. {l1_zips}, {usgs_level1_files}")
        # ODC_FILTERED_FILE
        usgs_level1_files = jobdir.joinpath(ODC_FILTERED_FILE)
        with open(usgs_level1_files, "w") as fid:
            fid.write("\n".join(l1_zips))

    if len(uuids2archive) > 0:
        # ARCHIVE_FILE
        path_scenes_to_archive = jobdir.joinpath(ARCHIVE_FILE)
        with open(path_scenes_to_archive, "w") as fid:
            fid.write("\n".join(uuids2archive))
        ard_click_params["archive-list"] = path_scenes_to_archive

    # write pbs script
    script_path = jobdir.joinpath(PBS_ARD_FILE)
    with open(script_path, "w") as src:
        src.write(make_ard_pbs(usgs_level1_files, **ard_click_params))

    # Make the script executable
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

    # run the script
    if run_ard is True:
        subprocess.run([script_path], check=True)

    LOGGER.info("info", jobdir=str(jobdir))
