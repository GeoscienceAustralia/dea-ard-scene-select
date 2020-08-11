#! /usr/bin/env python3

import datetime
import pytz
import random
from random import randrange

from scene_select.check_ancillary import definitive_ancillary_files

import time


def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)

# start_date = datetime.date(2002, 1, 1)
# end_date = datetime.date(2022, 1, 1)

# time_between_dates = end_date - start_date
# days_between_dates = time_between_dates.days

d_start = datetime.datetime(2001, 12, 31, tzinfo=pytz.UTC)
d_end = datetime.datetime(2020, 8, 1, tzinfo=pytz.UTC)

start = time.time()

for i in range(1000):
    a_date = random_date(d_start, d_end)
    # datetime.datetime(2001, 12, 31, tzinfo=pytz.UTC)
    definitive_ancillary_files(a_date)
end = time.time()
print(end - start)
