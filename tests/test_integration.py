#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path

from scene_select.ard_scene_select import scene_select, DATA_DIR
from scene_select.generate_aoi import generate_region

OZWRS = DATA_DIR.joinpath("Australian_Wrs_list.txt")
TEST_DATA_DIR = Path(__file__).parent.joinpath("test_data")
STANDARD_SCENES_SELECTED = TEST_DATA_DIR.joinpath("standard_scenes_to_ARD_process.txt")
L1EXAMPLES = TEST_DATA_DIR.joinpath("All_Landsat_Level1_Nci_Files.txt")
# The Worldwide Reference System (WRS) is a global notation system for Landsat data.
STANDARD_WRS_AOI = TEST_DATA_DIR.joinpath("standard_wrs_list.txt")
# actually, we done have the results file yet...


def test_scene_select_main():

    dirpath = tempfile.mkdtemp()
    scenes_filepath, all_scenes_list = scene_select.callback(
        usgs_level1_files=L1EXAMPLES,
        search_datacube=False,
        allowed_codes=OZWRS,
        nprocs=1,
        config=None,
        days_delta=None,
        products=None,
        logdir=dirpath,
        run_ard=False,
        nodes=None,
        walltime=None,
        workers=None,
        env=None,
        stop_logging=True,
        log_config=None,
        scene_limit=999999,
    )
    standard = set(line.strip() for line in open(STANDARD_SCENES_SELECTED))
    results = set(all_scenes_list)

    # Note this is just comparing the results from 2020-07-01 to when you ran the test
    # Good for stopping new errors coming in
    # Will not pick up anything bad before then
    # Plus this does not cover the ODC code.
    assert len(standard) == len(results)
    assert standard == results

    # ... do stuff with dirpath
    shutil.rmtree(dirpath)


def test_generate_aoi_main():

    dirpath = tempfile.mkdtemp()
    _, allowed_codes = generate_region.callback(satellite_data_provider="USGS", workdir=dirpath)
    standard = set(line.strip() for line in open(STANDARD_WRS_AOI))
    results = set(allowed_codes)

    # Note this is just comparing the results from the past to when you ran the test
    # Good for stopping new errors coming in
    # Will not pick up anything bad before then
    # Plus this does not cover the ODC code.
    assert standard == results

    # ... do stuff with dirpath
    shutil.rmtree(dirpath)
