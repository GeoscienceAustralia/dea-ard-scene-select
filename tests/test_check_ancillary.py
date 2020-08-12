#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path
import datetime
import pytz

from scene_select.check_ancillary import definitive_ancillary_files

BRDF_TEST_DIR = Path(__file__).parent.joinpath("test_data", "BRDF")
WV_TEST_DIR = Path(__file__).parent.joinpath("test_data", "water_vapour")


def test_definitive_ancillary_files_local():
    # no water v data for these years
    a_dt = datetime.datetime(1944, 6, 4, tzinfo=pytz.UTC)
    assert not definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)
    a_dt = datetime.datetime(2018, 6, 4, tzinfo=pytz.UTC)
    assert not definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)

    # no water v data - explore more
    a_dt = datetime.datetime(2020, 8, 13, tzinfo=pytz.UTC)
    assert not definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)

    #  water v data - no BRDF
    a_dt = datetime.datetime(2020, 8, 2, tzinfo=pytz.UTC)
    assert not definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)

    #  water v data - no BRDF - but that is ok, before BDF started
    a_dt = datetime.datetime(2002, 2, 2, tzinfo=pytz.UTC)
    assert definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)

    #  water v data  BRDF
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)
    assert definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)

    #  water v data  BRDF - different time zone format
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=datetime.timezone.utc)
    assert definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)
