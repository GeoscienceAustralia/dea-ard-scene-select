#! /usr/bin/env python3

import os
from typing import List
from pathlib import Path


DATA_DIR = Path(__file__).parent.joinpath("data")


def _get_allowed_path_rows(path_row_file: Path) -> List:
    with open(path_row_file, "r") as fid:
        return [
            "{:03}{:03}".format(
                int(item.rstrip().split("_")[0]), int(item.rstrip().split("_")[1])
            )
            for item in fid.readlines()
        ]


def _check_level1_lists(
    level1_list: Path, check_codes: dict, pathrows_allowed: List
) -> None:

    with open(level1_list, "r") as fid:
        level1_list = [
            os.path.basename(item.rstrip()).split("_") for item in fid.readlines()
        ]

    sensor = [item[0] for item in level1_list]

    # check results are only for one sensor
    assert len(set(sensor)) == 1

    # check only allowed processing levels are in the list
    if check_codes["sensor"] == "L08":
        assert set([item[1] for item in level1_list]) == set(
            check_codes["process_level"]
        )

    # check there no path/rows other than allowed list of path rows
    assert len(set([item[2] for item in level1_list]) - set(pathrows_allowed)) == 0


def main():
    level1_ls8 = DATA_DIR.joinpath("L08_CollectionUpgrade_Level1_list.txt")
    level1_ls7 = DATA_DIR.joinpath("L07_CollectionUpgrade_Level1_list.txt")
    level1_ls5 = DATA_DIR.joinpath("L08_CollectionUpgrade_Level1_list.txt")
    path_row_au = DATA_DIR.joinpath("Australian_Wrs_list.txt")

    allowed_pathrows = _get_allowed_path_rows(Path(path_row_au))

    # check landsat8 level1 lists
    _check_level1_lists(
        Path(level1_ls8),
        {"sensor": "L08", "process_level": ["L1TP", "L1GT"]},
        allowed_pathrows,
    )

    # check landsat7 level1 lists
    _check_level1_lists(
        Path(level1_ls7), {"sensor": "L07", "process_level": ["L1TP"]}, allowed_pathrows
    )

    # check landsat5 level1 lists
    _check_level1_lists(
        Path(level1_ls5), {"sensor": "L05", "process_level": ["L1TP"]}, allowed_pathrows
    )


if __name__ == "__main__":
    main()
