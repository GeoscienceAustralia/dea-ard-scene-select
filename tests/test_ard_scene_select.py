#! /usr/bin/env python3

import tempfile
from pathlib import Path
import datetime
import pytz
import re

from scene_select.ard_scene_select import (
    dict2ard_arg_string,
    exclude_days,
    _calc_nodes_req,
    _calc_node_with_defaults,
)


def test_dict2ard_arg_string():
    ard_click_params = {"index_datacube_env": "/g/data", "walltime": None}
    ard_arg_string = dict2ard_arg_string(ard_click_params)
    assert ard_arg_string == "--index-datacube-env /g/data"


def test_exclude_days():
    range1 = ["2020-08-09:2020-08-30", "2020-09-02:2020-09-05"]

    # not excluded
    a_dt = datetime.datetime(1944, 6, 4, tzinfo=pytz.UTC)
    assert not exclude_days(range1, a_dt)
    a_dt = datetime.datetime(2020, 9, 1, tzinfo=pytz.UTC)
    assert not exclude_days(range1, a_dt)
    a_dt = datetime.datetime(2020, 9, 6, tzinfo=pytz.UTC)
    assert not exclude_days(range1, a_dt)
    a_dt = datetime.datetime(
        2020, 8, 8, hour=23, minute=59, second=59, microsecond=999999, tzinfo=pytz.UTC,
    )
    assert not exclude_days(range1, a_dt)

    # excluded
    a_dt = datetime.datetime(
        2020, 8, 30, hour=23, minute=59, second=59, microsecond=999999, tzinfo=pytz.UTC,
    )
    assert exclude_days(range1, a_dt)
    a_dt = datetime.datetime(2020, 8, 9, tzinfo=pytz.UTC)
    assert exclude_days(range1, a_dt)

    range2 = ["2020-08-09:2020-08-09"]
    # excluded
    a_dt = datetime.datetime(
        2020, 8, 9, hour=23, minute=59, second=59, microsecond=999999, tzinfo=pytz.UTC,
    )
    assert exclude_days(range1, a_dt)
    a_dt = datetime.datetime(2020, 8, 9, tzinfo=pytz.UTC)
    assert exclude_days(range1, a_dt)


def test_exclude_days_diff_tzinfo():
    range1 = ["2020-08-09:2020-08-30", "2020-09-02:2020-09-05"]

    # not excluded
    a_dt = datetime.datetime(1944, 6, 4, tzinfo=datetime.timezone.utc)
    assert not exclude_days(range1, a_dt)


def test_exclude_days_empty():
    range1 = []

    # not excluded
    a_dt = datetime.datetime(1944, 6, 4, tzinfo=datetime.timezone.utc)
    assert not exclude_days(range1, a_dt)


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


def test_calc_nodes_req():
    ard_click_params = {"walltime": "1:00:00", "nodes": None, "workers": None}
    count_all_scenes_list = 1

    try:
        _calc_node_with_defaults(ard_click_params, count_all_scenes_list)
    except ValueError as err:
        assert len(err.args) >= 1


L8_C2_PATTERN = (
    r"^(?P<sensor>LC)"
    r"(?P<satellite>08)_"
    r"(?P<processingCorrectionLevel>L1TP|L1GT)_"
    r"(?P<wrsPath>[0-9]{3})"
    r"(?P<wrsRow>[0-9]{3})_"
    r"(?P<acquisitionDate>[0-9]{8})_"
    r"(?P<processingDate>[0-9]{8})_"
    r"(?P<collectionNumber>02)_"
    r"(?P<collectionCategory>T1|T2)"
    r"(?P<extension>)$"
)


def test_L8_PATTERN():
    landsat_product_id = "LC08_L1TP_089078_20211026_20211104_02_T1"
    if not re.match(L8_C2_PATTERN, landsat_product_id):
        print(re.match(L8_C2_PATTERN, landsat_product_id))
        assert False
    landsat_product_id = "LC08_L1TP_094073_20211014_20211019_02_T1"
    if not re.match(L8_C2_PATTERN, landsat_product_id):
        print(re.match(L8_C2_PATTERN, landsat_product_id))
        assert False
