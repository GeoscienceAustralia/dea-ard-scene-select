"""
    This test suite aims at ensuring changes to
scene_select() are tested for maintainability and
accuracy on how the function is to work.

"""

from pathlib import Path
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


pytestmark = pytest.mark.usefixtures("auto_odc_db")


def test_using_auto_odc_db(tmp_path):
    """
    If there is no explicit jobdir, DASS will create
    a default directory to be used.
    To run scene_select, we are passing in a file
    containing all level-1 USGS/ESA entries to be filtered.
    """
    cmd_params = [
        "--logdir",
        tmp_path,
    ]

    runner = CliRunner()
    result = runner.invoke(
        scene_select,
        cmd_params,
    )
    assert result.exit_code == 0, (
        f"Scene_select process failed to execute  {result.output}"
    )

    # Depending on the type of error, the info on the
    # error will either be in result.exception or result.output.
    # result.output usually captures system errors whilst
    # result.exception will capture errors with expected arguments.
    # This usually traps process related errors such as
    # missing arguments.
    assert result.exception is None, (
        f" Exception thrown in {result.exception}/{result.output}"
    )
