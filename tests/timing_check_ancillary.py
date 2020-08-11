#! /usr/bin/env python3

import datetime
import pytz
import random

from scene_select.check_ancillary import definitive_ancillary_files

import time


start_date = datetime.date(2002, 1, 1)
end_date = datetime.datetime.now()

time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days
start = time.time()

for i in range(1000):
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    print(random_date)
end = time.time()
print(end - start)