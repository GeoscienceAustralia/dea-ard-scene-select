#! /usr/bin/env python3

import tempfile
import shutil
import datetime
import pytz
import re
import pytest
import os
import uuid
from pathlib import Path
from click.testing import CliRunner
from scene_select.ard_scene_select import (
    exclude_days,
    scene_select,
)


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


# Set this to true and all the results
# of the scene select run will be displayed
VERBOSE = False


@pytest.fixture(scope="function")
def temp_directory():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


DATAFILE_DIR = Path(__file__).parent.joinpath("test_data").resolve()


def test_scene_select_with_explicit_jobdir_no_db():
    """
    Given an explicit jobdir which does not exist,
    we expect that directory to be created and used.
    To run scene_select, we are passing in a file
    containing all level-1 USGS/ESA entries to be filtered.
    """

    # Specify the directory path
    custom_jobdir = "temp_jobdir_testing_" + str(uuid.uuid4()).replace("-", "")

    temp_dir = "temp_logdir_testing_" + str(uuid.uuid4()).replace("-", "")
    os.mkdir(temp_dir)

    cmd_params = [
        "--usgs-level1-files",
        DATAFILE_DIR / "All_Landsat_Level1_Nci_Files.txt",
        "--jobdir",
        custom_jobdir,
        "--logdir",
        temp_dir,
    ]

    try:
        runner = CliRunner()
        result = runner.invoke(
            scene_select,
            cmd_params,
        )

        if VERBOSE:
            print("RUNNING ARD SCENE SELECT")
            print("***** results output ******")
            print(result.output)
            print("***** results exception ******")
            print(f"'{result.exception}'")
            print("***** results end ******")

        # Depending on the type of error, the info on the
        # error will either be in result.exception or result.output.
        # result.output usually captures system errors whilst
        # result.exception will capture errors with expected arguments.
        # This usually traps process related errors such as
        # missing arguments.
        assert (
            result.exception is None
        ), f" Exception thrown in {result.exception}/{result.output}"

        # Assert that when presented, the jobdir flag is accepted
        assert (
            "Error: No such option: --jobdir" not in result.output
        ), "scene_select() doesn't recognise the job dir attribute"

        # Assert that the file exists
        assert os.path.exists(
            custom_jobdir
        ), f"Failed: Custom job directory, '{custom_jobdir}' does not exist"

    except Exception as exception_message:
        # this traps errors thrown out by the scene_select() function
        # such as bad parameter types or NoneType when the argument
        # is expected to be non-None
        pytest.fail(f"Unexpected exception: {exception_message}")
    # clean up
    shutil.rmtree(temp_dir)
    shutil.rmtree(custom_jobdir)


def test_scene_select_no_explicit_jobdir_no_db():
    """
    Given no mention of jobdir, we expect the scene
    select to not throw an exception.
    To run scene_select, we are passing in a file
    containing all level-1 USGS/ESA entries to be filtered.
    Under the hood, a default directory to be used.
    It will be made from an extract of a unique id
    (given by a package called uuid) thus there is
    no way we could get the jobid from the outside.
    Based on ard reprocessed l1s:
    jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    """

    temp_dir = "temp_logdir_testing_" + str(uuid.uuid4()).replace("-", "")
    os.mkdir(temp_dir)
    # Clean up: Can't delete the temp directory (in job dir) even if we wanted
    # to because it's been created by scene_select() which has its
    # permissions and scope on it. Thus, this test doesn't have visibility
    # on the temp directory

    cmd_params = [
        "--usgs-level1-files",
        DATAFILE_DIR / "All_Landsat_Level1_Nci_Files.txt",
        "--logdir",
        temp_dir,
    ]

    try:
        runner = CliRunner()
        result = runner.invoke(
            scene_select,
            cmd_params,
        )

        if VERBOSE:
            print("RUNNING ARD SCENE SELECT")
            print("***** results output ******")
            print(result.output)
            print("***** results exception ******")
            print(result.exception)
            print("***** results end ******")

        # Depending on the type of error, the info on the
        # error will either be in result.exception or result.output.
        # result.output usually captures system errors whilst
        # result.exception will capture errors with expected arguments.
        # This usually traps process related errors such as
        # missing arguments.
        assert (
            result.exception is None
        ), f" Exception thrown in {result.exception}/{result.output}"

    except Exception as exception_message:
        # this traps errors thrown out by the scene_select() function
        # such as bad parameter types or NoneType when the argument
        # is expected to be non-None
        pytest.fail(f"Unexpected exception: {exception_message}")
    # clean up
    shutil.rmtree(temp_dir)