#!/usr/bin/env python3

from unittest.mock import Mock
from scene_select.utils import calc_file_path, load_skip_list


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


def test_load_skip_list(tmp_path):
    skip_file = tmp_path / "skip.csv"
    skip_file.write_text(
        "landsat_product_id,reason\n"
        "LC08_L1GT_135097_20221203_20221212_02_T2,No DSM coverage (SR-2231)\n"
        "\n"
        "# Awaiting a USGS fix, remove after their next release:\n"
        "LE07_L1TP_091081_20200101_20200823_02_T1,Corrupt band 3\n"
        # A reason containing a comma, and one containing a newline.
        'S2A_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10,"Broken, somehow"\n'
        'S2B_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10,"Two\nlines"\n'
        # Trailing whitespace, and a row without a reason at all.
        "  LT05_L1TP_092084_19880413_20200917_02_T1  ,\n"
        "LT05_L1TP_092085_19880413_20200917_02_T1\n"
    )

    assert load_skip_list(skip_file) == {
        "LC08_L1GT_135097_20221203_20221212_02_T2",
        "LE07_L1TP_091081_20200101_20200823_02_T1",
        "S2A_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10",
        "S2B_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10",
        "LT05_L1TP_092084_19880413_20200917_02_T1",
        "LT05_L1TP_092085_19880413_20200917_02_T1",
    }


def test_load_skip_list_without_header(tmp_path):
    """A bare list of ids (no header, no reasons) should work too."""
    skip_file = tmp_path / "skip.csv"
    skip_file.write_text(
        "LC08_L1GT_135097_20221203_20221212_02_T2\n"
        "LE07_L1TP_091081_20200101_20200823_02_T1\n"
    )

    assert load_skip_list(skip_file) == {
        "LC08_L1GT_135097_20221203_20221212_02_T2",
        "LE07_L1TP_091081_20200101_20200823_02_T1",
    }


def test_load_skip_list_empty(tmp_path):
    skip_file = tmp_path / "skip.csv"
    skip_file.write_text("product_id,reason\n")
    assert load_skip_list(skip_file) == set()
