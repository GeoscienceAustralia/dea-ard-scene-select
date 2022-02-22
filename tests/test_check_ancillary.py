#! /usr/bin/env python3

import datetime
from pathlib import Path

from wagl.hdf5 import H5CompressionFilter  # don't delete, needed 4 plugins
import pytz

from scene_select.check_ancillary import AncillaryFiles

__all__ = ("H5CompressionFilter",)  # Stop flake8 F401's

BRDF_TEST_DIR = Path(__file__).parent.joinpath("test_data", "BRDF")
WV_TEST_DIR = Path(__file__).parent.joinpath("test_data", "water_vapour")


def test_ancillaryfiles_local():
    # no water v data for these years
    af_ob = AncillaryFiles(brdf_dir=BRDF_TEST_DIR, wv_dir=WV_TEST_DIR)
    a_dt = datetime.datetime(1944, 6, 4, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert "year" in msg
    assert not ancill_there

    a_dt = datetime.datetime(2018, 6, 4, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert "year" in msg
    assert not ancill_there

    # no water v data - explore more
    a_dt = datetime.datetime(2020, 8, 13, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert "Water vapour" in msg
    assert not ancill_there

    #  water v data - no BRDF
    a_dt = datetime.datetime(2020, 8, 2, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert "BRDF" in msg
    assert not ancill_there

    #  water v data - no BRDF - but that is ok, before BDF started
    a_dt = datetime.datetime(2002, 2, 2, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert ancill_there

    #  water v data  BRDF
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert ancill_there

    #  water v data  BRDF - different time zone format
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=datetime.timezone.utc)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert ancill_there


def test_ancillaryfiles_water():

    # BRDF there. last day out of wv data
    af_ob = AncillaryFiles(brdf_dir=BRDF_TEST_DIR, wv_dir=WV_TEST_DIR)
    a_dt = datetime.datetime(2020, 8, 9, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert ancill_there

    #  BRDF there. one day out from wv data
    a_dt = datetime.datetime(2020, 8, 10, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert ancill_there

    #  BRDF there. two days out from wv data
    a_dt = datetime.datetime(2020, 8, 11, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert "Water vapour" in msg
    assert not ancill_there
