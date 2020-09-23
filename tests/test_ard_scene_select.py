#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path

from scene_select.ard_scene_select import dict2ard_arg_string, allowed_codes_to_region_codes


def test_dict2ard_arg_string():
    ard_click_params = {"index_datacube_env": "/g/data", "walltime": None}
    ard_arg_string = dict2ard_arg_string(ard_click_params)
    assert ard_arg_string == "--index-datacube-env /g/data"


def test_allowed_codes_to_region_codes():
    file_path = Path(tempfile.mkdtemp(prefix="testrun"), "config.yaml")
    with file_path.open(mode="w") as f:
        f.write("102_67\n")
        f.write("102_68\n")
    the_file_path = str(file_path)
    path_row_list = allowed_codes_to_region_codes(the_file_path)
    assert path_row_list == ["102067", "102068"]
