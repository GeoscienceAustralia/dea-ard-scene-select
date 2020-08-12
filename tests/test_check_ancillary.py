#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path
import datetime
import pytz

from scene_select.check_ancillary import definitive_ancillary_files

BRDF_TEST_DIR = Path(__file__).parent.joinpath("testdata", "BRDF")
WV_TEST_DIR = Path(__file__).parent.joinpath("testdata", "water_vapour")

def test_definitive_ancillary_files_local():
    # Crap test, since it is relying on a location on gadi and certain files being there
    # No water v files
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
    
    #  water v data and BRDF
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)
    assert definitive_ancillary_files(a_dt, brdf_dir=BRDF_TEST_DIR, water_vapour_dir=WV_TEST_DIR)
    
def test_definitive_ancillary_files():
    # Crap test, since it is relying on a location on gadi and certain files being there
    assert not definitive_ancillary_files(datetime.datetime(1944, 6, 4, tzinfo=pytz.UTC))
    assert definitive_ancillary_files(datetime.datetime(2001, 12, 31, tzinfo=pytz.UTC))
    assert definitive_ancillary_files(datetime.datetime(2003, 10, 11, tzinfo=pytz.UTC))
    assert definitive_ancillary_files(datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC))


def test_definitive_ancillary_files_different_utc():
    # Crap test, since it is relying on a location on gadi and certain files being there
    assert not definitive_ancillary_files(datetime.datetime(1944, 6, 4, tzinfo=datetime.timezone.utc))
    assert definitive_ancillary_files(datetime.datetime(2001, 12, 31, tzinfo=datetime.timezone.utc))
    assert definitive_ancillary_files(datetime.datetime(2003, 10, 11, tzinfo=datetime.timezone.utc))
    assert definitive_ancillary_files(datetime.datetime(2020, 8, 1, tzinfo=datetime.timezone.utc))


def test_definitive_ancillary_files_the_future():
    # Crap test, since it is relying on a location on gadi and certain files being there
    # Introducing my first YK3 bug
    assert not definitive_ancillary_files(datetime.datetime(3000, 10, 11, tzinfo=pytz.UTC))
