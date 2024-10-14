#! /usr/bin/env python3

import datetime
import pytz
import re
import os
from pathlib import Path
from click.testing import CliRunner
from scene_select.ard_scene_select import (
    exclude_days,
    scene_select,
)

DATAFILE_DIR = Path(__file__).parent.joinpath("test_data").resolve()


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
        2020,
        8,
        8,
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        tzinfo=pytz.UTC,
    )
    assert not exclude_days(range1, a_dt)

    # excluded
    a_dt = datetime.datetime(
        2020,
        8,
        30,
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        tzinfo=pytz.UTC,
    )
    assert exclude_days(range1, a_dt)
    a_dt = datetime.datetime(2020, 8, 9, tzinfo=pytz.UTC)
    assert exclude_days(range1, a_dt)

    range2 = ["2020-08-09:2020-08-09"]
    # excluded
    a_dt = datetime.datetime(
        2020,
        8,
        9,
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        tzinfo=pytz.UTC,
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


def test_scene_select_with_explicit_jobdir(tmp_path):
    """
    Given an explicit jobdir which does not exist,
    DASS will create and use the dir.
    To run scene_select, we are passing in a file
    containing all level-1 USGS/ESA entries to be filtered.
    """

    # Specify the directory path

    custom_jobdir = tmp_path / "dir_dass_will_create"

    cmd_params = [
        "--usgs-level1-files",
        DATAFILE_DIR / "All_Landsat_Level1_Nci_Files.txt",
        "--jobdir",
        custom_jobdir,
        "--logdir",
        tmp_path,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        cmd_params,
    )
    assert (
        result.exit_code == 0
    ), f"Scene_select process failed to execute  {result.output}"

    # Depending on the type of error, the info on the
    # error will either be in result.exception or result.output.
    # result.output usually captures system errors whilst
    # result.exception will capture errors with expected arguments.
    # This usually traps process related errors such as
    # missing arguments.
    assert (
        result.exception is None
    ), f" Exception thrown in {result.exception}/{result.output}"

    # Assert that the file exists
    assert os.path.exists(
        custom_jobdir
    ), f"Failed: Custom job directory, '{custom_jobdir}' does not exist"


def test_scene_select_no_explicit_jobdir(tmp_path):
    """
    If there is no explicit jobdir, DASS will create
    a default directory to be used.
    To run scene_select, we are passing in a file
    containing all level-1 USGS/ESA entries to be filtered.
    """
    cmd_params = [
        "--usgs-level1-files",
        DATAFILE_DIR / "All_Landsat_Level1_Nci_Files.txt",
        "--logdir",
        tmp_path,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        cmd_params,
    )
    assert (
        result.exit_code == 0
    ), f"Scene_select process failed to execute  {result.output}"

    # Depending on the type of error, the info on the
    # error will either be in result.exception or result.output.
    # result.output usually captures system errors whilst
    # result.exception will capture errors with expected arguments.
    # This usually traps process related errors such as
    # missing arguments.
    assert (
        result.exception is None
    ), f" Exception thrown in {result.exception}/{result.output}"
