"""
    This test suite aims at ensuring changes to
scene_select() are tested for maintainability and
accuracy on how the function is to work.

"""

import shutil
from pathlib import Path
import uuid
import os
import pytest
from click.testing import CliRunner

from scene_select.ard_scene_select import (
    scene_select,
)

ODC_FILES_DIR = Path(__file__).parent.joinpath("..", "test_data", "odc_setup").resolve()
METADATA_TYPES = [
    ODC_FILES_DIR / "metadata/eo3_landsat_l1.odc-type.yaml",
    ODC_FILES_DIR / "metadata/eo3_landsat_ard.odc-type.yaml",
]
PRODUCTS = [
    ODC_FILES_DIR / "eo3/l1_ls7.odc-product.yaml",
    ODC_FILES_DIR / "eo3/l1_ls7.odc-product.yaml",
    ODC_FILES_DIR / "eo3/l1_ls8_c2.odc-product.yaml",
    ODC_FILES_DIR / "eo3/l1_ls9.odc-product.yaml",
    ODC_FILES_DIR / "eo3/ard_ls8.odc-product.yaml",
    ODC_FILES_DIR / "eo3/ard_ls7.odc-product.yaml",
    ODC_FILES_DIR / "eo3/ard_ls9.odc-product.yaml",
]

DATAFILE_DIR = (
    Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
)
DATASET = [
    DATAFILE_DIR
    / "l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml",
    DATAFILE_DIR
    / "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
    "/g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml",
    "/g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml",
    DATAFILE_DIR
    / "l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml",
    DATAFILE_DIR
    / "ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml",
    DATAFILE_DIR
    / "ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml",
    DATAFILE_DIR
    / "l1_Landsat_C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml",
    DATAFILE_DIR
    / "ga_ls9c_ard_3/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml",
]  # add


# Set this to true and all the results
# of the scene select run will be displayed
VERBOSE = False

pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_scene_select_with_explicit_jobdir_with_db():
    """
    Given an explicit jobdir which does not exist,
    we expect that directory to be created and used.
    """
    custom_jobdir = "testing_jobdir_" + str(uuid.uuid4()).replace("-", "")

    logdir = "testing_logdir_" + str(uuid.uuid4()).replace("-", "")
    os.mkdir(logdir)

    cmd_params = [
        "--logdir",
        logdir,
        "--jobdir",
        custom_jobdir,
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

        # Assert that when presented, the jobdir flag is accepted
        assert (
            "Error: No such option: --jobdir" not in result.output
        ), "scene_select() doesn't recognise the job dir attribute"

        assert (
            len(os.listdir(custom_jobdir)) > 0
        ), f" Nothing is inside the custom job directory, {custom_jobdir}"

        # Assert that the file exists
        assert os.path.exists(custom_jobdir), (
            "Failed: Custom job dir, '" + str(custom_jobdir) + "' does not exist"
        )
    except Exception as exception_message:
        # this traps errors thrown out by the scene_select() function
        # such as bad parameter types or NoneType when the argument
        # is expected to be non-None
        pytest.fail(f"Unexpected exception: {exception_message}")
    # clean up
    shutil.rmtree(logdir)
    shutil.rmtree(custom_jobdir)


def test_scene_select_without_explicit_jobdir_with_db():
    """
    Given no mention of jobdir, we expect the scene
    select to not throw an exception.
    Under the hood, a default directory to be used.
    It will be made from an extract of a unique id
    (given by a package called uuid) thus there is
    no way we could get the jobid from the outside.
    Based on ard reprocessed l1s:
    jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    """
    logdir = "testing_logdir_" + str(uuid.uuid4()).replace("-", "")
    os.mkdir(logdir)
    try:
        cmd_params = [
            "--logdir",
            logdir,
        ]

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
