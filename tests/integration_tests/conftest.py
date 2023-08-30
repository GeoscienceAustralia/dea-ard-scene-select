"""
     Common fixtures to support integration tests
"""

import shutil
from typing import Tuple
from pathlib import Path
import pytest


@pytest.fixture(scope="function")
def setup_local_directories_and_files() -> Tuple[str, str]:
    """
    Create a scratch and pkg directories
    """
    current_directory = Path(".")

    # Create a new directory named "AutomatedTestRunPackage"
    scratch_dir = current_directory / "AutomatedTestRunScratch"

    # Create the directory if it doesn't exist
    scratch_dir.mkdir(exist_ok=True)

    # Create a new directory named "AutomatedTestRunPackage"
    package_dir = current_directory / "AutomatedTestRunPackage"

    # Create the directory if it doesn't exist
    package_dir.mkdir(exist_ok=True)

    yield (scratch_dir, package_dir)

    shutil.rmtree(scratch_dir)
    shutil.rmtree(package_dir)
