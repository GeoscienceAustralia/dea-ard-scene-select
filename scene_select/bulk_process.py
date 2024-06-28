"""
Create a PBS Job to (re)process ARD datasets in bulk

This is a simpler alternative to scene_select, intended for bulk jobs
that replace existing data.

(It is intended to have good defaults, so you don't need to remember our AOI, where our Level 1 lives on NCI,
or where logs should go, etc... )

This will search, filter the scenes, and create the ard_pbs script. You can then run the result if it looks good.

The first argument is which "collections" to process. This can be "ls", "s2", or more specific matches
like "ls7" or "s2a"*

 (*prefix is globbed on ARD product names as "ga_{prefix}*", so "ls" is expanded to "ga_ls*", etc)

Nothing is actually run by default, it simply creates a work directory and scripts to kickoff.

Optionally, you can provide standard ODC search expressions to limit the scenes chosen.

Normal ODC expressions will be passed to the ODC search function, except for any field
with suffix `_version`. It will be compared against the proc-info file software versions.

Expression examples:

    platform = LANDSAT_8

    time in 2014-03-02
    time in 2014-3-2
    time in 2014-3
    time > 2014
    time in [2014, 2014]
    time in [2014-03-01, 2014-04-01]
    time > 2014

    lat in [4, 6] time in 2014-03-02
    lat in [4, 6]

    wagl_version in ["1.2.3", "3.4.5"]
    wagl_version < "1.2.3.dev4"
    fmask_version = "4.2.0"

    platform=LS8 lat in [-14, -23.5] instrument="OTHER"

Examples:

    # Any five sentinel2 scenes

    ard-bulk-reprocess s2  --max-count 5

    # Landsat scenes for a given month that are below a gqa value

    ard-bulk-reprocess ls  'time in 2023-04'  'wagl_version>"0.1.3"'  --max-count 1000

Logs are printed to stderr by default. It will be in readable (coloured) form if to a
live terminal, or json otherwise.

You can redirect stderr if you want to record logs:

    ard-bulk-reprocess s2  --max-count 500 2> bulk-reprocess.jsonl

"""

import os
import shlex
import stat
from pathlib import Path
from textwrap import dedent
from typing import List, Dict

import click
import structlog
from datacube import Datacube
from datacube.model import Range
from datacube.ui import click as ui

from scene_select.do_ard import calc_node_with_defaults
from scene_select.collections import get_collection
from packaging import version

from scene_select.library import Level1Dataset
from scene_select.scene_filters import parse_expressions, GreaterThan, LessThan
from scene_select.utils import structlog_setup

DEFAULT_WORK_DIR = Path("/g/data/v10/work/bulk-runs")


def expression_parse(ctx, param, value):
    return parse_expressions(*list(value))


@click.command("ard-bulk-reprocess", help=__doc__)
@ui.environment_option
@ui.config_option
@click.argument("prefix")
@click.argument("expressions", callback=expression_parse, nargs=-1)
@click.option("--max-count", default=100, help="Maximum number of scenes to process")
@click.option(
    "--work-dir",
    type=Path,
    default=DEFAULT_WORK_DIR,
    help="Base folder for working files (will create subfolders for each job)",
)
@click.option(
    "--pkg-dir",
    type=Path,
    default=None,
    help="Output package base path (default: work-dir/pkg)",
)
@ui.pass_index(app_name="bulk-reprocess")
def cli(
    index, prefix: str, max_count: int, work_dir: Path, pkg_dir: Path, expressions: dict
):
    import wagl

    current_wagl_version = wagl.__version__

    software_expressions = pop_software_expressions(expressions)

    if len(prefix) < 2:
        raise ValueError(
            "Sorry, prefix needs to be at least ls or s2, as we can't process multiple sensors in one go (yet)"
        )
    platform = prefix[:2]
    if platform not in ("ls", "s2"):
        raise ValueError(
            f"Expected collection to begin with either ls or s2, got {prefix}"
        )
    environment_file = f"/g/data/v10/work/landsat_downloads/landsat-downloader/config/dass-prod-wagl-{platform}.env"

    structlog_setup()

    log = structlog.get_logger()

    with Datacube(index=index) as dc:
        collection = get_collection(dc, prefix)

        log.info("chosen_products", products=[c.name for c in collection.products])
        # Filter to our set of ARD products.

        # The level1s to process, and the ids of datasets that will be replaced by them.
        level1s_to_process: Dict[Level1Dataset, List[str]] = {}
        unique_products = set()

        for ard_product, ard_dataset in collection.iterate_indexed_ard_datasets(
            expressions
        ):
            log = log.bind(dataset_id=ard_dataset.dataset_id)
            if not ard_dataset.metadata_path.exists():
                log.warning("dataset_missing_from_disk")
                continue

            if not matches_software_expressions(
                ard_dataset.software_versions(), software_expressions, log=log
            ):
                continue

            level1 = dc.index.datasets.get(ard_dataset.level1_id)
            if level1 is None:
                log.warning(
                    "skip.source_level1_not_indexed", dataset_id=ard_dataset.dataset_id
                )
                # TODO: Perhaps a newer one exists? Or on disk?
                continue

            # TODO: Does a newer Level 1 exist? We'd rather use that.
            level1_product = [
                s for s in ard_product.sources if s.name == level1.product.name
            ][0]

            level1_dataset = Level1Dataset.from_odc(level1, level1_product)
            level1s_to_process.setdefault(level1_dataset, []).append(
                str(ard_dataset.dataset_id)
            )
            unique_products.add(ard_product)

            if len(level1s_to_process) >= max_count:
                log.info("reached_max_dataset_count", max_count=max_count)
                break

        from datetime import datetime

        job_directory = (
            work_dir / platform / datetime.now().strftime("%Y-%m/%Y-%m-%d-%H%M%S")
        )
        job_directory = job_directory.resolve()
        job_directory.mkdir(parents=True, exist_ok=False)
        scene_list_path = job_directory / "scene-level1-path-list.txt"
        scene_archive_path = job_directory / "scene-archive-list.csv"

        # Write file of level1s to process.
        with scene_list_path.open("w") as fid:
            for level1 in level1s_to_process.keys():
                fid.write(str(level1.data_path) + "\n")

        # And a list of ARDs to archive (folder and uuid)?
        with scene_archive_path.open("w") as fid:
            for level1_dataset, uuids_to_archive in level1s_to_process.items():
                fid.write(f'{level1_dataset.data_path},{",".join(uuids_to_archive)}\n')

        dirs = dict(
            workdir=job_directory / "run",
            pkgdir=pkg_dir or job_directory / "pkg",
            # ard-pbs creates subfolders for each batch log anyway.
            logdir=job_directory,
        )
        for _, dir_path in dirs.items():
            dir_path.mkdir(parents=True, exist_ok=True)

        if level1_product.separate_metadata_directory:
            dirs["yamls-dir"] = level1_product.separate_metadata_directory
        ard_args = dict(
            project="v10",
            walltime="10:00:00",
            env=environment_file,
            nodes=None,
            workers=None,
            **dirs,
        )
        calc_node_with_defaults(ard_args, len(level1s_to_process))
        pbs = dedent(f"""
            #!/bin/bash

            module purge
            module load pbs

            source {environment_file}

            ard_pbs --level1-list {scene_list_path} {dict_to_cli_args(ard_args, multiline_indent=16)}
        """).lstrip()
        script_path = job_directory / "run_ard_pbs.sh"
        with open(script_path, "w") as src:
            src.write(pbs)
        os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

        log.info(
            "created_job",
            dataset_count=len(level1s_to_process),
            products=len(unique_products),
            job_directory=str(job_directory),
            script_path=str(script_path),
        )


def dict_to_cli_args(args: dict, multiline_indent=None) -> str:
    """
    >>> dict_to_cli_args({"this_env": "env123", "complex_key": "a complex key"})
    "--this-env env123 --complex-key 'a complex key'"
    >>> print(dict_to_cli_args(dict(k1=1, k2=2, k3=3, k4=4), multiline_indent=4))
    --k1 1 \\
        --k2 2 \\
        --k3 3 \\
        --k4 4
    """
    ard_params = []
    for key, value in args.items():
        if not value:
            continue

        key = key.replace("_", "-")
        ard_params.append(
            f"--{key} {shlex.quote(str(value))}",
        )
    if multiline_indent:
        s_indent = " " * multiline_indent
        ard_arg_string = f" \\\n{s_indent}".join(ard_params)
    else:
        ard_arg_string = " ".join(ard_params)
    return ard_arg_string


def pop_software_expressions(expressions: dict) -> dict:
    """
    Any key ending in `_version` is removed from the expressions and returned as a separate dict.
    """
    software_expressions = {}
    for key in list(expressions.keys()):
        if key.endswith("_version"):
            software_expressions[key] = expressions.pop(key)
    return software_expressions


def matches_software_expressions(
    software_versions: dict, software_expressions: dict, log
) -> bool:
    """
    Check if the software versions match the expressions provided.
    """
    log.debug("software_versions", software_versions=software_versions)

    for key, value in software_expressions.items():
        # "wagl_version" key should correspond to software called "wagl"
        key = key[: -len("_version")]

        if key not in software_versions:
            log.error("skip.missing_software_version", key=key)
            return False
        dataset_version = version.parse(software_versions[key])
        if isinstance(value, Range):
            if value.begin and dataset_version < version.parse(value.begin):
                log.debug(
                    "skip.software_version_too_low",
                    key=key,
                    expected_range=value,
                    actual=software_versions[key],
                )
                return False
            if value.end and dataset_version > version.parse(value.end):
                log.debug(
                    "skip.software_version_too_high",
                    key=key,
                    expected_range=value,
                    actual=software_versions[key],
                )
                return False
        elif isinstance(value, GreaterThan):
            if dataset_version <= version.parse(value.value):
                log.debug(
                    "skip.software_version_too_low",
                    key=key,
                    expected=value,
                    actual=software_versions[key],
                )
                return False
        elif isinstance(value, LessThan):
            if dataset_version >= version.parse(value.value):
                log.debug(
                    "skip.software_version_too_high",
                    key=key,
                    expected=value,
                    actual=software_versions[key],
                )
                return False
        else:
            if dataset_version != version.parse(value):
                log.debug(
                    "skip.software_version_mismatch",
                    key=key,
                    expected=value,
                    actual=software_versions[key],
                )
                return False
    return True


def test_matches_software_expressions():
    assert matches_software_expressions(
        dict(wagl="1.2.3", fmask="4.2.0"),
        dict(wagl_version="1.2.3", fmask_version="4.2.0"),
        structlog.get_logger(),
    )
    assert not matches_software_expressions(
        dict(wagl="1.2.3", fmask="4.2.0"),
        dict(wagl_version="1.2.4", fmask_version="4.2.0"),
        structlog.get_logger(),
    )

    # Ranges
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.3", None)),
        structlog.get_logger(),
    )
    assert not matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.4", None)),
        structlog.get_logger(),
    )
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.3", "1.2.4")),
        structlog.get_logger(),
    )


if __name__ == "__main__":
    cli()
