#!/usr/bin/env python3

from urllib.parse import urlparse
from urllib.request import url2pathname
from pathlib import Path

import click

DATA_DIR = Path(__file__).parent.joinpath("data")

# Logging
LOG_CONFIG_FILE = "log_config.ini"


def calc_file_path(l1_dataset, product_id):
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


def calc_local_path(l1_dataset):
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