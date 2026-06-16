#!/usr/bin/env python3

from unittest.mock import Mock
from scene_select.utils import calc_file_path


def test_local_path():
    # Sentinel 2 should return the inner zip's s3 location without a zip:// uri
    s2_l1_dataset = Mock()
    s2_l1_dataset.local_path = None
    s2_l1_dataset.uris = [
        "zip:s3://ourdata/g/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip!/"
    ]
    result = calc_file_path(s2_l1_dataset)
    assert (
        result
        == "s3://ourdata/g/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip"
    )

    # Landsat should find a sibling tar file in s3.
    ls_l1_dataset = Mock()
    ls_l1_dataset.local_path = None
    ls_l1_dataset.uris = [
        "s3://our-data/LC80990702026160/LC08_L1TP_099070_20260609_20260613_02_T1.odc-metadata.yaml"
    ]
    result = calc_file_path(ls_l1_dataset)
    assert (
        result
        == "s3://our-data/LC80990702026160/LC08_L1TP_099070_20260609_20260613_02_T1.tar"
    )
