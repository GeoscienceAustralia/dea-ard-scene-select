#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path
import datetime

from scene_select.check_ancillary import definitive_ancillary_files


def test_definitive_ancillary_files():
    # Crap test, since it is relying on a location on gadi and certain files being there
    # It even relies on todays files not being there!
    assert not definitive_ancillary_files(datetime.datetime(1944, 6, 4))
    assert definitive_ancillary_files(datetime.datetime(2001, 12, 31))
    assert definitive_ancillary_files(datetime.datetime(2003, 10, 11))
    assert not definitive_ancillary_files(datetime.datetime.now())
