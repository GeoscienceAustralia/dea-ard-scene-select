#!/usr/bin/env python3

import math
from pathlib import Path
import subprocess
import os
import stat
from typing import TypedDict, Optional

import structlog

_LOG = structlog.get_logger()


class ArdParameters(TypedDict, total=False):
    walltime: Optional[str]  # Job walltime in `hh:mm:ss` format
    email: Optional[str]  # Notification email address
    project: str  # Project code to run under, default="v10"
    logdir: Optional[str]  # The base logging and scripts output directory
    jobdir: Optional[str]  # The start ard processing directory
    pkgdir: Optional[str]  # The base output packaged directory
    yamls_dir: Optional[
        str
    ]  # folder to find yaml files (if they aren't in same location as data)
    env: Optional[str]  # Environment script to source
    index_datacube_env: Optional[str]  # Path to the datacube indexing environment
    workers: Optional[int]  # The number of workers to request per node (1-48)
    nodes: Optional[str]  # The number of nodes to request
    memory: Optional[str]  # The memory in GB to request per node
    jobfs: Optional[str]  # The jobfs memory in GB to request per node
    nodes: Optional[int]  # The number of nodes to request

    archive_list: Optional[
        str
    ]  # The path containing a list of UUIDs to archive on success


ODC_FILTERED_FILE = "level1_paths_for_ard.txt"
PBS_ARD_FILE = "run_ard_pbs.sh"
PBS_JOB = """#!/bin/bash
module purge
module load pbs

source {env}

ard_pbs --level1-list {scene_list} {ard_args}
"""


def calc_node_with_defaults(ard_click_params: ArdParameters, count_all_scenes_list):
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


def _calc_nodes_req(
    granule_count: int, walltime: str, workers: int, hours_per_granule=7.5
) -> int:
    """Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59:00', 28)
    6
    >>> _calc_nodes_req(800, '20:00', 28)
    11
    >>> _calc_nodes_req(800, '20:00', 48)
    7
    """
    hours, mins, *secs = (float(x) for x in walltime.split(":"))
    hours = hours + (mins / 60.0)
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


def generate_ard_job(
    ard_click_params: ArdParameters,
    l1_count: int,
    uuids2archive: list,
    jobdir: Path,
    run_ard: bool,
    l1_paths=None,
    l1_paths_file: Path = None,
):
    """Create a PBS job for an ARD job and optionally run it.

    Either give a list of L1 paths, or a file path containing the list of L1 paths.
    """
    _LOG.info("do_ard", **locals())
    try:
        calc_node_with_defaults(ard_click_params, l1_count)
    except ValueError as err:
        print(err.args)
        _LOG.warning("ValueError", message=err.args)

    if l1_paths:
        if l1_paths_file:
            raise ValueError("Specify either l1_paths or l1_paths_file, not both")
        l1_paths_file = jobdir.joinpath(ODC_FILTERED_FILE)
        with open(l1_paths_file, "w") as fid:
            fid.write("\n".join(l1_paths))
            fid.write("\n")

    if len(uuids2archive) > 0:
        path_scenes_to_archive = jobdir.joinpath("ard_uuids_to_archive.txt")
        with open(path_scenes_to_archive, "w") as fid:
            fid.write("\n".join(uuids2archive))
        ard_click_params["archive_list"] = path_scenes_to_archive.resolve().as_posix()

    # write pbs script
    script_path = jobdir.joinpath(PBS_ARD_FILE)
    with open(script_path, "w") as src:
        src.write(make_ard_pbs(l1_paths_file.resolve(), **ard_click_params))

    # Make the script executable
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

    # run the script
    if run_ard is True:
        subprocess.run([script_path], check=True)

    _LOG.info("info", jobdir=str(jobdir))
