#!/usr/bin/env python3

import json

"""
Script to convert Landsat and S2 AOI's into a json format.

The format for the Landsat AOI is a list of path_row strings;
89_83 
or 
089_084
"""


def load_file(file_name):
    """load a file, return a list."""
    with open(file_name) as f:
        lines = f.read().splitlines()
    return lines


landsat = "Australian_wrs_list_extended.txt"
with open(landsat) as fid:
    path_row_list = [line.rstrip() for line in fid.readlines()]
path_row_list = [
    "{:03}{:03}".format(int(item.split("_")[0]), int(item.split("_")[1]))
    for item in path_row_list
]

s2 = "Australian_tile_list_optimised.txt"
tiles = load_file(s2)
region_dic = {"ls": path_row_list, "s2": tiles}
output_file = "Australian_AOI_ls_extended.json"

with open(output_file, "w") as f:
    json.dump(region_dic, f, ensure_ascii=False, indent=4)

# load example
with open(output_file, "r") as f:
    data = json.load(f)

print(data)
