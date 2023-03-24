#! /usr/bin/env python3

import tempfile
import datetime
import pytz
import re

from scene_select.ard_scene_select import exclude_days


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


def test_l8_pattern():
    landsat_product_id = "LC08_L1TP_089078_20211026_20211104_02_T1"
    if not re.match(L8_C2_PATTERN, landsat_product_id):
        print(re.match(L8_C2_PATTERN, landsat_product_id))
        assert False
    landsat_product_id = "LC08_L1TP_094073_20211014_20211019_02_T1"
    if not re.match(L8_C2_PATTERN, landsat_product_id):
        print(re.match(L8_C2_PATTERN, landsat_product_id))
        assert False


S2_PATTERN = r"^(?P<satellite>S2)" + r"(?P<satelliteid>[A-B])_"


def test_s2_pattern():
    landsat_product_id = "LC08_L1TP_089078_20211026_20211104_02_T1"
    if not re.match(L8_C2_PATTERN, landsat_product_id):
        print(re.match(L8_C2_PATTERN, landsat_product_id))
        assert False
