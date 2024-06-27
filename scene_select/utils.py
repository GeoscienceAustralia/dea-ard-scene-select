#!/usr/bin/env python3
import logging
import os
import sys
from pathlib import Path
from typing import TextIO

from urllib.parse import urlparse
from urllib.request import url2pathname
from subprocess import Popen, PIPE

import click
import structlog

from datacube.model import Dataset

DATA_DIR = Path(__file__).parent.joinpath("data")

# Logging
LOG_CONFIG_FILE = "log_config.ini"
LOG_CONFIG = DATA_DIR.joinpath(LOG_CONFIG_FILE)

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
    Remove the groundstation/version information from a scene id.

    >>> chopped_scene_id('LE71800682013283ASA00')
    'LE71800682013283'
    """
    if len(scene_id) != 21:
        raise RuntimeError(f"Unsupported scene_id format: {scene_id!r}")
    capture_id = scene_id[:-5]
    return capture_id


class PythonLiteralOption(click.Option):
    """Load click value representing a Python list."""

    def type_cast_value(self, ctx, value):
        try:
            value = str(value)
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
    cmd_results = {}

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


def structlog_setup(output: TextIO | None = sys.stderr):
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
            structlog.processors.JSONRenderer(),
        ]
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=output),
        cache_logger_on_first_use=False,
    )
