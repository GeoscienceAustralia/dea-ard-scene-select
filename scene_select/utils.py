#!/usr/bin/env python3
import functools
import logging
import os
import re
import sys
import traceback
from pathlib import Path, PurePath
from typing import TextIO

from urllib.parse import urlparse
from urllib.request import url2pathname
from subprocess import Popen, PIPE
import datetime
from typing import Tuple, Iterator
import calendar
import click

from datacube.model import Range, Dataset
import structlog


DATA_DIR = Path(__file__).parent.joinpath("data")

# Logging
LOG_CONFIG_FILE = "log_config.ini"
LOG_CONFIG = DATA_DIR.joinpath(LOG_CONFIG_FILE)

EXPECTED_CHOPPED_S2_PATTERN = re.compile(r"S2[A-C]_L1C_[A-Z0-9]{6}_[0-9]{8}T[0-9]{6}")

INSIGNIFICANT_DIGITS_FIX = [
    "--allow-any",
    "extent.lon.end",
    "--allow-any",
    "extent.lon.begin",
    "--allow-any",
    "extent.lat.end",
    "--allow-any",
    "extent.lat.begin",
]


def calc_file_path(l1_dataset: Dataset, product_id: str) -> str:
    if l1_dataset.local_path is None:
        # The s2 way
        file_path = calc_local_path(l1_dataset)
    else:
        # The ls way
        local_path = l1_dataset.local_path

        # Metadata assumptions
        a_path = local_path.parent.joinpath(product_id)
        file_path = a_path.with_suffix(".tar").as_posix()
    return file_path


def calc_local_path(l1_dataset: Dataset) -> str:
    assert len(l1_dataset.uris) == 1, str(l1_dataset.uris)
    components = urlparse(l1_dataset.uris[0])
    if not (components.scheme == "file" or components.scheme == "zip"):
        raise ValueError(
            "Only file/Zip URIs currently supported. Tried %r." % components.scheme
        )
    path = url2pathname(components.path)
    if path[-2:] == "!/":
        path = path[:-2]
    return path


def chopped_scene_id(scene_id: str) -> str:
    """
    Create a string to uniquely identify an acquisition within the collection.
    >>> chopped_scene_id('LE71800682013283ASA00')
    'LE71800682013283'
    >>> chopped_scene_id('S2A_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10')
    'S2A_L1C_T56JLN_20240129T005713'
    """
    if scene_id.startswith("S"):
        return chop_s2_tile_id(scene_id)
    elif scene_id.startswith("L"):
        return chopped_ls_scene_id(scene_id)
    else:
        raise NotImplementedError(f"Unsupported scene_id format: {scene_id!r}")


def chopped_ls_scene_id(scene_id: str) -> str:
    """
    Create a string to uniquely identify an LS acquisition within the collection.

    ie. chop off their processing version number.

    >>> chopped_ls_scene_id('LE71800682013283ASA00')
    'LE71800682013283'
    """
    if len(scene_id) != 21:
        raise RuntimeError(f"Unsupported scene_id format: {scene_id!r}")
    capture_id = scene_id[:-5]
    return capture_id


def chop_s2_tile_id(sentinel_tile_id: str) -> str:
    """
    Create a string to uniquely identify an S2 acquisition within the collection.

    (for instance, we remove processing time, because a reprocessed acquisition will be a duplicate.)

    The chosen fields are based on GA's naming conventions:

        /ga_s2am_ard_3/56/JLN/2024/01/29/20240129T005713/ga_s2am_ard_3-2-1_56JLN_2024-01-29_final.odc-metadata.yaml

    (if it was acquired from the same groundstation, or had the same processing time, it would clash in name, because
    they are not included.)

    >>> chop_s2_tile_id('S2A_OPER_MSI_L1C_TL_2APS_20240129T005713_A044929_T56JLN_N05.10')
    'S2A_L1C_T56JLN_20240129T005713'
    """
    split_tile_id = sentinel_tile_id.strip().split("_")
    if len(split_tile_id) != 10:
        raise NotImplementedError(
            f"Unexpected sentinel_tile_id format: {sentinel_tile_id!r}"
        )

    # This all feels dangerous, which is why we check the result with a regexp below.
    sensor = split_tile_id[0]
    level = split_tile_id[3]
    datatake_date = split_tile_id[-4]
    region_code = split_tile_id[-2]

    code = f"{sensor}_{level}_{region_code}_{datatake_date}"

    # Let's be safe -- loud error if some have a different tile format.
    if not EXPECTED_CHOPPED_S2_PATTERN.match(code):
        raise NotImplementedError(f"Unexpected chopped S2 code: {code!r}")

    return code


class PythonLiteralOption(click.Option):
    """
    Load click value representing a Python list.

    This previously required the entire python list syntax, but this is considered legacy. It's an
    escaping nightmare.

    Instead, separate values by comma.
    """

    def type_cast_value(self, ctx, value):
        value = str(value)
        if "[" not in value:
            # Assume simple comma-separated items.
            return [item.strip() for item in value.split(",")]
        else:
            # This is considered legacy, but included for now for backwards compatibility.
            try:
                assert value.count("[") == 1
                assert value.count("]") == 1
                list_str = value.replace('"', "'").split("[")[1].split("]")[0]
                l_items = [item.strip().strip("'") for item in list_str.split(",")]
                if l_items == [""]:
                    l_items = []
                return l_items
            except Exception:
                raise click.BadParameter(value)


def scene_move(current_path: Path, current_base_path: str, new_base_path: str):
    """
    Move a scene from one location to another and update the odc database.
    Assume the dea module has been loaded.

    returning
        worked : bool if False then the move failed and the scene was not moved
        cmd_results : A dict with the following keys
            cmd : str the command that was run
            status : Int From the database update call 0 is success
            outs : str output from the database update call
            errs  : str output from the database update call
    """
    worked = True

    dst = new_base_path / current_path.relative_to(current_base_path)
    os.makedirs(dst.parent, exist_ok=True)
    os.rename(current_path.parent, dst.parent)

    # pylint: disable=W0105
    """
        # This did not work. Keeping a record of it here, for future improvement.
        from datacube.index.hl import Doc2Dataset

        # This produced many Warnings. Lets stick with calling the cmd.
        with dst.open("r") as f:
            doc = yaml.safe_load(f)
        with Datacube(app="usgs-l1-dl") as dc:
            (dataset, error_message) = Doc2Dataset(dc.index)(doc, dst.as_uri())
            dc.index.datasets.update(dataset)
    """

    cmd = ["datacube", "dataset", "update", str(dst), "--location-policy", "forget"]
    # This avoids update failures due to
    # minor differences in the extent metadata
    cmd += INSIGNIFICANT_DIGITS_FIX
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    outs, errs = proc.communicate()
    status = int(proc.returncode)
    if status != 0:
        # Move the scene data back to the original location
        os.rename(dst.parent, current_path.parent)
        worked = False
    update_results = {
        "cmd": " ".join(cmd),
        "status": str(status),
        "outs": str(outs),
        "errs": str(errs),
    }
    return worked, update_results


def structlog_setup(output: TextIO | None = sys.stderr, verbose=False):
    """
    Sensible structlog defaults.

    It will pretty-print if going to an interactive terminal, and otherwise output json.

    You can manually give a file to output to.

    :param output: file to print to. (default: `sys.stderr`)
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]

    if output.isatty():
        # Pretty printing when run in a terminal session.
        # Automatically prints pretty tracebacks when "rich" is installed
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(sort_keys=False),
        ]
    else:
        # Log JSON when run otherwise
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(default=_lenient_json_default),
        ]
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.NOTSET if verbose else logging.INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=output),
        cache_logger_on_first_use=False,
    )


def _lenient_json_default(o):
    """
    A json-dump `default` function that will show
    pathlib Paths as normal strings
    """

    if isinstance(o, PurePath):
        return o.as_posix()

    return repr(o)


def month_as_range(year: int, month: int) -> Range:
    """
    >>> month_as_range(2024, 2)
    Range(begin=datetime.datetime(2024, 2, 1, 0, 0), end=datetime.datetime(2024, 2, 29, 23, 59, 59, 999999))
    >>> month_as_range(2023, 12)
    Range(begin=datetime.datetime(2023, 12, 1, 0, 0), end=datetime.datetime(2023, 12, 31, 23, 59, 59, 999999))
    """
    week_day, number_of_days = calendar.monthrange(year, month)
    return Range(
        datetime.datetime(year, month, 1),
        datetime.datetime(year, month, number_of_days, 23, 59, 59, 999999),
    )


def iterate_months(
    start_time: datetime.date, end_time: datetime.date
) -> Iterator[Tuple[int, int]]:
    """
    Yield every month between the two times as a pair of (year, month) tuples

    Both sides are inclusive.
    """
    start_year, start_month = start_time.year, start_time.month
    end_year, end_month = end_time.year, end_time.month

    current_year, current_month = start_year, start_month

    while (current_year, current_month) <= (end_year, end_month):
        yield current_year, current_month

        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1


class LogAnyErrors:
    """
    Catch any exceptions from the main function and include in logs.
    """

    def __init__(self, logger):
        self.logger = logger

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as ex:
                self.logger(
                    "exception",
                    exception=ex.__str__(),
                    traceback=traceback.format_exc().splitlines(),
                )
                raise ex
            return result

        return decorated
