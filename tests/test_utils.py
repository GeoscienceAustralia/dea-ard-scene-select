#!/usr/bin/env python3

from pathlib import Path
from unittest.mock import Mock
from scene_select.utils import calc_file_path


def test_local_path():
    # s2
    # uris = ["zip:/yada/yada/yada20124T021536.zip!/"]

    # ls
    # "local_path": "PosixPath('/g/data/u46/yada_01_T2.odc-metadata.yaml')
    s2_l1_dataset = Mock()
    s2_l1_dataset.local_path = None
    path = "/g/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip"
    s2_l1_dataset.uris = ["zip:" + path + "!/"]
    product_id = "S2A_OPER_MSI_L1C_TL_VGS2_20220124T021536_A034419_T54LYH_N03.01"
    result = calc_file_path(s2_l1_dataset, product_id)
    assert result == path

    ls_l1_dataset = Mock()
    the_path = "/this/path/"
    ls_l1_dataset.local_path = Path(the_path + "LC08_T2.odc-metadata.yaml")
    product_id = "LC08_T2"
    actual = the_path + product_id + ".tar"
    result = calc_file_path(ls_l1_dataset, product_id)
    assert result == actual
