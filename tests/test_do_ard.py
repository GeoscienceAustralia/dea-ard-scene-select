#!/usr/bin/env python3

import os

from scene_select.do_ard import (
    do_ard,
    ARCHIVE_FILE,
    ODC_FILTERED_FILE,
    PBS_ARD_FILE,
    dict2ard_arg_string,
    _calc_nodes_req,
    calc_node_with_defaults,
)


def test_dict2ard_arg_string():
    ard_click_params = {"index_datacube_env": "/g/data", "walltime": None}
    ard_arg_string = dict2ard_arg_string(ard_click_params)
    assert ard_arg_string == "--index-datacube-env /g/data"


def test_calc_nodes_req():
    granule_count = 400

    walltime = "20:59:00"
    workers = 28
    hours_per_granule = 1.5
    results = _calc_nodes_req(granule_count, walltime, workers, hours_per_granule)
    assert results == 2

    granule_count = 800
    walltime = "20:00:00"
    workers = 28
    hours_per_granule = 1.5
    results = _calc_nodes_req(granule_count, walltime, workers, hours_per_granule)
    assert results == 3

    granule_count = 1
    walltime = "01:00:00"
    workers = 1
    hours_per_granule = 7
    results = _calc_nodes_req(granule_count, walltime, workers, hours_per_granule)
    print(results)


def test_calc_nodes_req_more():
    ard_click_params = {"walltime": "1:00:00", "nodes": None, "workers": None}
    count_all_scenes_list = 1

    try:
        calc_node_with_defaults(ard_click_params, count_all_scenes_list)
    except ValueError as err:
        assert len(err.args) >= 1


def test_do_ard(tmp_path):
    ard_click_params = {
        "email": None,
        "env": "/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/tests/integration_tests/ls_interim_prod_wagl.env",
        "index_datacube_env": None,
        "jobfs": None,
        "memory": None,
        "nodes": None,
        "pkgdir": "scratch_ls/pkgdir23061",
        "project": "u46",
        # "test": false,
        "walltime": "02:30:00",
        "workdir": "scratch_ls/",
        "workers": None,
        "yamls_dir": "",
    }
    usgs_level1_files = None
    l1_zips = ["scene1.zip", "scene2.zip", "scene3.zip"]
    l1_count = len(l1_zips)
    uuids2archive = ["uuid1", "uuid2", "uuid3"]
    jobdir = tmp_path / "jobdir"
    jobdir.mkdir()
    run_ard = False
    do_ard(
        ard_click_params,
        l1_count,
        usgs_level1_files,
        uuids2archive,
        jobdir,
        run_ard,
        l1_zips,
    )

    # Not checking the values, just that the files exist
    assert os.path.exists(tmp_path / "jobdir" / ARCHIVE_FILE), (
        ARCHIVE_FILE + " does not exist."
    )
    assert os.path.exists(tmp_path / "jobdir" / ODC_FILTERED_FILE), (
        ODC_FILTERED_FILE + " does not exist."
    )
    assert os.path.exists(tmp_path / "jobdir" / PBS_ARD_FILE), (
        PBS_ARD_FILE + " does not exist."
    )
