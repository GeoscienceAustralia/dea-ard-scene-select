#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path

from scene_select.ard_scene_select import main, DATA_DIR

L1EXAMPLES = DATA_DIR.joinpath("All_Landsat_Level1_Nci_Files.txt")
OZWRS = DATA_DIR.joinpath("Australian_Wrs_list.txt")
TEST_DATA_DIR = Path(__file__).parent.joinpath("test_data")
STANDARD_SCENES_SELECTED = TEST_DATA_DIR.joinpath("standard_scenes_to_ARD_process.txt")
# actually, we done have the results file yet...


def test_main():

    dirpath = tempfile.mkdtemp()
    scenes_filepath, all_scenes_list = main.callback(
    satellite_data_provider="ESA",
    usgs_level1_files=L1EXAMPLES,
    search_datacube=False,
    allowed_codes=OZWRS,
    nprocs=1,
    config=None,
    days_delta=None,
    products=None,
    workdir=dirpath,
    run_ard=False,
    landsat_aoi=True,
    nodes=None,
    walltime=None,
    workers=None,
    env=None)
    standard = set(line.strip() for line in open(STANDARD_SCENES_SELECTED))
    results = set(all_scenes_list)

    # Note this is just comparing the results from 2020-07-01 to when you ran the test
    # Good for stopping new errors coming in
    # Will not pick up anything bad before then
    # Plus this does not cover the ODC code.
    assert standard == results

    # ... do stuff with dirpath
    shutil.rmtree(dirpath)