#! /usr/bin/env python3

import tempfile
import shutil
from pathlib import Path
import datetime
import pytz

from scene_select.check_ancillary import definitive_ancillary_files


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

def Xtest_definitive_ancillary_files_baaad():
    # Crap test, since it is relying on a location on gadi and certain files being there
    #assert definitive_ancillary_files(datetime.datetime(2020, 7, 3, tzinfo=pytz.UTC))
    #assert definitive_ancillary_files(datetime.datetime(2020, 8, 3, tzinfo=pytz.UTC))
    #assert definitive_ancillary_files(datetime.datetime(2020, 8, 4, tzinfo=pytz.UTC))
    assert not definitive_ancillary_files(datetime.datetime(2020, 8, 5, tzinfo=pytz.UTC))
    #assert not definitive_ancillary_files(datetime.datetime(2020, 8, 6, tzinfo=pytz.UTC))
    #assert not definitive_ancillary_files(datetime.datetime(2020, 8, 7, tzinfo=pytz.UTC))


def test_definitive_ancillary_files_the_future():
    # Crap test, since it is relying on a location on gadi and certain files being there
    # Introducing my first YK3 bug
    assert not definitive_ancillary_files(datetime.datetime(3000, 10, 11, tzinfo=pytz.UTC))
