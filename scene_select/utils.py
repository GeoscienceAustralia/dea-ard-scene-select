#!/usr/bin/env python3
import csv
import logging
import os
import re
import sys
from pathlib import Path, PurePath
from typing import Set, TextIO

from subprocess import Popen, PIPE

import click
import structlog

from datacube.model import Dataset

DATA_DIR = Path(__file__).parent.joinpath("data")

# Logging
LOG_CONFIG_FILE = "log_config.ini"
LOG_CONFIG = DATA_DIR.joinpath(LOG_CONFIG_FILE)

EXPECTED_CHOPPED_S2_PATTERN = re.compile(r"S2[A-C]_L1C_[A-Z0-9]{6}_[0-9]{8}T[0-9]{6}")

# First-column names we'll treat as a header row in a skip list, rather than as an id.
SKIP_LIST_HEADER_NAMES = frozenset(
    {
        "id",
        "landsat_product_id",
        "landsat_scene_id",
        "level1_id",
        "product_id",
        "scene_id",
        "sentinel_tile_id",
        "tile_id",
    }
)

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


def calc_file_path(l1_dataset: Dataset) -> str:
    """Get the input file path for processing.

    Dataset locations are the prefix URI to reading data.

    - Sentinel-2: zip archive stored as zip+s3 uri prefix: 'zip:s3://file.zip!/'
    - Landsat: s3 location of the metadata yaml: convert it to sibling tar file.
    """
    # In AWS we don't use multiple URIs
    assert len(l1_dataset.uris) == 1

    uri = l1_dataset.uris[0]

    # Sentinel-2: convert zip+s3 uri to just an s3 link to the zip.
    if uri.startswith("zip:s3"):
        uri = uri.replace("zip:s3", "s3")
    # Remove zip/tar inner-file suffix to get the outer file.
    if uri.endswith("!/"):
        uri = uri[:-2]

    # If a metadata file is indexed, we expect the data as a sibling file.
    # Landsat is indexed this way for its tar files.
    if uri.endswith(".odc-metadata.yaml"):
        return uri.replace(".odc-metadata.yaml", ".tar")

    return uri


def load_skip_list(file_path: Path) -> Set[str]:
    """
    Load a set of level-1 ids that should never be processed.

    Datasets fail in the processor for reasons we can't fix (bad source data, no
    elevation coverage, ...), and would otherwise be retried on every single run.
    An operator lists them here to take them out of scene select permanently.

    The file is a CSV whose first column is a level-1 id: either a
    `landsat_product_id` or a `sentinel_tile_id`, as reported in our logs. Any
    remaining columns are ignored, so operators can record why each scene was
    skipped, and whether it's expected to be temporary.

    An optional header row is allowed, as are blank lines and `#` comments::

        landsat_product_id,reason
        LC08_L1GT_135097_20221203_20221212_02_T2,No DSM coverage (SR-2231)
        # Awaiting a USGS fix, remove after their next release:
        LE07_L1TP_091081_20200101_20200823_02_T1,Corrupt band 3

    The whole file is held in memory as a set of strings: a 200,000 scene list
    is a few tens of MB, and lookups stay O(1).
    """
    skip_ids = set()
    seen_a_row = False

    with open(file_path, "r", newline="") as f:
        for row in csv.reader(f):
            # csv gives [] for a blank line.
            if not row:
                continue
            identifier = row[0].strip()
            if not identifier or identifier.startswith("#"):
                continue

            if not seen_a_row:
                seen_a_row = True
                if identifier.lower() in SKIP_LIST_HEADER_NAMES:
                    continue

            skip_ids.add(identifier)

    return skip_ids


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
