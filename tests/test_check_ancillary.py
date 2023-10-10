#! /usr/bin/env python3

import datetime
from pathlib import Path

try:
    from wagl.hdf5 import H5CompressionFilter  # don't delete, needed 4 plugins
except ModuleNotFoundError:
    # for non-NCI setup
    import tables
import pytz

from scene_select.check_ancillary import AncillaryFiles

__all__ = ("H5CompressionFilter",)  # Stop flake8 F401's

BRDF_TEST_DIR = Path(__file__).parent.joinpath("test_data", "BRDF")
VIIRS_I_TEST_DIR = Path(__file__).parent.joinpath("test_data", "VNP43IA1.001")
VIIRS_M_TEST_DIR = Path(__file__).parent.joinpath("test_data", "VNP43MA1.001")
WV_TEST_DIR = Path(__file__).parent.joinpath("test_data", "water_vapour")


def test_viirs():
    # I and M for this date
    af_ob = AncillaryFiles(
        viirs_m_path=VIIRS_M_TEST_DIR,
        viirs_i_path=VIIRS_I_TEST_DIR,
        brdf_dir=BRDF_TEST_DIR,
        wv_dir=WV_TEST_DIR,
    )
    a_dt = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)
    ymd = a_dt.strftime("%Y.%m.%d")
    ancill_there, msg = af_ob.check_viirs(ymd)
    assert ancill_there
    assert msg == "", "Unexpectedly, there is a returned message"

    # I_viirs only for this date
    af_ob = AncillaryFiles(
        viirs_m_path=VIIRS_M_TEST_DIR,
        viirs_i_path=VIIRS_I_TEST_DIR,
        brdf_dir=BRDF_TEST_DIR,
        wv_dir=WV_TEST_DIR,
    )
    a_dt = datetime.datetime(2020, 8, 2, tzinfo=pytz.UTC)
    ymd = a_dt.strftime("%Y.%m.%d")
    ancill_there, msg = af_ob.check_viirs(ymd)
    assert not ancill_there, "Unexpectedly, there is brdf vii I data"
    assert msg != "", "Unexpectedly, there is no returned message"

    # M only for this date
    af_ob = AncillaryFiles(
        viirs_m_path=VIIRS_M_TEST_DIR,
        viirs_i_path=VIIRS_I_TEST_DIR,
        brdf_dir=BRDF_TEST_DIR,
        wv_dir=WV_TEST_DIR,
    )
    a_dt = datetime.datetime(2020, 8, 3, tzinfo=pytz.UTC)
    ymd = a_dt.strftime("%Y.%m.%d")
    ancill_there, msg = af_ob.check_viirs(ymd)
    assert not ancill_there
    assert not ancill_there, "Unexpectedly, there is brdf vii I data"


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


def test_ancillaryfiles_actual():

    # BRDF there. last day out of wv data
    af_ob = AncillaryFiles()
    a_dt = datetime.datetime(1944, 8, 10, tzinfo=pytz.UTC)
    ancill_there, msg = af_ob.ancillary_files(a_dt)
    assert not ancill_there
    assert "year" in msg
