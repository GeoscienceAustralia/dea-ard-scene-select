#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path

from scene_select.ard_scene_select import dict2ard_arg_string


def test_dict2ard_arg_string():
    ard_click_params = {"index_datacube_env": "/g/data", "walltime": None}
    ard_arg_string = dict2ard_arg_string(ard_click_params)
    assert ard_arg_string == "--index-datacube-env /g/data"
