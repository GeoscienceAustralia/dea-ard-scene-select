#!/usr/bin/env python3

"""
Testing connecting to a ODC database using
pytest-odc

Note,
The docker package has been removed
and a try statement has been put around import docker in
database.py
"""

from pathlib import Path
import os

import pytest

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

if True:
    user_id = os.environ["USER"]
    os.environ[
        "ODC_TEST_DB_URL"
    ] = f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"

if False:
    # For pytest-odc the ODC DB location is
    # set using ODC_TEST_DB_URL
    # This crashes trying to use docker.
    # Set environment variables for the test
    # Set the DATACUBE_ENVIRONMENT and DATACUBE_CONFIG_PATH
    #os.environ["DATACUBE_CONFIG_PATH"] = str(Path(__file__).parent.joinpath("datacube.conf"))
    user_id = os.environ["USER"]
    os.environ["DATACUBE_ENVIRONMENT"] = f"{user_id}_automated_testing"    

@pytest.fixture
def setup_environment_variables(autouse=True, scope="session"):
    """
    This is not working for me.
    It is not called before the odc_test_db fixture """
    user_id = os.environ["USER"]
    os.environ[
        "ODC_TEST_DB_URL"
    ] = f"postgresql://{user_id}@deadev.nci.org.au/{user_id}_automated_testing"


def test_add_dataset(setup_environment_variables, odc_test_db):
    odc_test_db.index.datasets.get("b49869c4-a65d-4f1f-b673-0f6a4bb8e090", include_sources=True)

    TEST_DIR = Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
    print(TEST_DIR)
