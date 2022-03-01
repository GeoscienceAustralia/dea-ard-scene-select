#!/usr/bin/env python3

import json

"""
Script to convert two text file AOI's into a json format.
"""


def load_file(file_name):
    """load a file, return a list."""
    with open(file_name) as f:
        lines = f.read().splitlines()
    return lines


landsat = "Australian_wrs_list_optimal_v2.txt"
with open(landsat) as fid:
    path_row_list = [line.rstrip() for line in fid.readlines()]
path_row_list = [
    "{:03}{:03}".format(int(item.split("_")[0]), int(item.split("_")[1]))
    for item in path_row_list
]

s2 = "Australian_tile_list_optimised.txt"
tiles = load_file(s2)
region_dic = {"ls": path_row_list, "s2": tiles}
output_file = "Australian_AOI.json"

with open(output_file, "w") as f:
    json.dump(region_dic, f, ensure_ascii=False, indent=4)

# load example
with open(output_file, "r") as f:
    data = json.load(f)

print(data)
