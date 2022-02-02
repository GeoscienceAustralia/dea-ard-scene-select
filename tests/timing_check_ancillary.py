#! /usr/bin/env python3

import datetime
import time
from pathlib import Path
from random import SystemRandom

from wagl.hdf5 import H5CompressionFilter
import pytz

from scene_select.check_ancillary import AncillaryFiles

__all__ = ("H5CompressionFilter",)  # Stop flake8 F401's

safe_random = SystemRandom()

BRDF_TEST_DIR = Path(__file__).parent.joinpath("test_data", "BRDF")
WV_TEST_DIR = Path(__file__).parent.joinpath("test_data", "water_vapour")


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = safe_random.randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)


# start_date = datetime.date(2002, 1, 1)
# end_date = datetime.date(2022, 1, 1)

# time_between_dates = end_date - start_date
# days_between_dates = time_between_dates.days

d_start = datetime.datetime(2001, 12, 31, tzinfo=pytz.UTC)
d_end = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)

af_ob = AncillaryFiles(brdf_dir=BRDF_TEST_DIR, wv_dir=WV_TEST_DIR)

t_start = time.time()

# Use 1000 for 5 sec at nci
# Use 170000 on laptop
for _ in range(1000):
    a_date = random_date(d_start, d_end)
    # datetime.datetime(2001, 12, 31, tzinfo=pytz.UTC)
    af_ob.ancillary_files(a_date)
t_end = time.time()
print(t_end - t_start)
