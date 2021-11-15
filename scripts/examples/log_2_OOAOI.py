#!/usr/bin/env python3

"""
OOAOI - Out of Australian area of interest.
"""

import json

log_file = (
    "/g/data1a/u46/users/dsg547/sandpit/dea-ard-scene-select/scripts/"
    "examples/scratch/filter-jobid-e515b2/ard_scene_select.log"
)


uuids = []
a_sum = 0
with open(log_file) as f:
    for line in f:
        info = json.loads(line)
        # print (line_dict)
        if "reason" in info and info["reason"] == "Region not in AOI":
            a_sum += 1
            uuid_long = info["uuid"]
            uuid_list = uuid_long.split("'")
            uuids.append(uuid_list[1])

print(a_sum)

f_out = open("myfile.txt", "w")
for a_uuid in uuids:
    f_out.write(a_uuid + "\n")
f_out.close()
