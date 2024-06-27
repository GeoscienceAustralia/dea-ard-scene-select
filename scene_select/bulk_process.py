"""
Create a PBS Job to (re)process dataset in bulk

This is a simpler alternative to scene_select, intended for bulk jobs
that replace existing data.

(It is intended to have good defaults, so you don't need to remember our AOI, where our Level 1 lives on NCI,
or where logs should go)

There's no logic to fill gaps etc, as you're planning
to replace scenes.

This will search, filter the scenes, and create the jobs.

Example:

    ard-bulk-reprocess collection=ls7 'ard<1.2.3' 'fmask<1.2.3'
"""

import os
import shlex
import stat
from pathlib import Path
from textwrap import dedent
from typing import List, Dict

import click
from datacube import Datacube

from scene_select.do_ard import calc_node_with_defaults
from scene_select.collections import ArdCollection, Level1Dataset
import logging
from packaging import version

WORK_DIR = Path("/g/data/v10/work/bulk-process")


#
# ard-bulk-reprocess ls7
#


@click.command("ard-bulk-reprocess")
@click.argument("prefix")
@click.option("--max-count", default=10, help="Maximum number of scenes to process")
def cli(prefix: str, max_count: int):
    import wagl

    current_wagl_version = wagl.__version__

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

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("scene_select.collections").setLevel(logging.DEBUG)

    log = logging.getLogger("ard-bulk-reprocess")

    with Datacube() as dc:
        collection = ArdCollection(dc, prefix)
        log.info(f"Choosing products {[c.name for c in collection.products]}")
        # Filter to our set of ARD products.

        # The level1s to process, and the ids of datasets that will be replaced by them.
        level1s_to_process: Dict[Level1Dataset, List[str]] = {}
        unique_products = set()

        for ard_product, ard_dataset in collection.iterate_indexed_ard_datasets():
            if not ard_dataset.metadata_path.exists():
                log.warning(f"{ard_dataset.dataset_id} does not exist on disk")
                continue

            processed_with_wagl_version = ard_dataset.software_versions()["wagl"]

            if not is_before_our_version(
                current_wagl_version, processed_with_wagl_version
            ):
                log.info(
                    f"{ard_dataset.dataset_id} already processed with a recent version of wagl"
                )
                continue

            # TODO: Other filtering as needed? eg, fmask.

            level1 = dc.index.datasets.get(ard_dataset.level1_id)
            if level1 is None:
                log.warning(
                    f"Source level 1 {ard_dataset.level1_id} not found in index"
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
                break

        from datetime import datetime

        job_directory = (
            WORK_DIR / platform / datetime.now().strftime("%Y-%m/%Y-%m-%d-%H%M%S")
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
            pkgdir=job_directory / "pkg",
            logdir=job_directory / "log",
        )
        for _, dir_path in dirs.items():
            dir_path.mkdir(parents=True, exist_ok=False)

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

        log.info(f"Job directory for {len(level1s_to_process)} datasets: {job_directory}")
        log.info(f"Runnable: {script_path}")


def is_before_our_version(our_version: str, their_version: str) -> bool:
    return version.parse(their_version) < version.parse(our_version)


def dict_to_cli_args(args: dict, multiline_indent=4) -> str:
    """
    >>> dict_to_cli_args({"this_env": "env123", "complex_key": "a complex key"})
    "--this-env env123 --complex-key 'a complex key'"
    >>> print(dict_to_cli_args(dict(k1=1, k2=2, k3=3, k4=4), multiline_indent=True))
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


if __name__ == "__main__":
    cli()
