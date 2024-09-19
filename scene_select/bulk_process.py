"""
Create a PBS Job to (re)process ARD datasets in bulk

This is a simpler alternative to scene_select, intended for bulk jobs
that replace existing data.

(It is intended to have good defaults, so you don't need to remember our AOI, where our Level 1 lives on NCI,
or where logs should go, etc... )


"""

import os
import shlex
import stat
import sys
from collections import defaultdict
from datetime import datetime
from itertools import chain
from pathlib import Path
from textwrap import dedent
from typing import List, Dict

import click
import structlog
from attr import define, field
from datacube import Datacube
from datacube.index.abstract import AbstractIndex
from datacube.model import Range, Dataset
from datacube.ui import click as ui
from packaging import version

from scene_select.collections import get_collection, get_product
from scene_select.do_ard import calc_node_with_defaults
from scene_select.library import Level1Dataset, ArdProduct, ArdCollection, ArdDataset
from scene_select.scene_filters import parse_expressions, GreaterThan, LessThan
from scene_select.utils import structlog_setup

DEFAULT_WORK_DIR = Path("/g/data/v10/work/bulk-runs")


def expression_parse(ctx, param, value):
    return parse_expressions(*list(value))


@define(unsafe_hash=True)
class Job:
    level1: Level1Dataset = field(eq=True, hash=False)
    replacement_uuids: List[str] = field(eq=False, hash=False)
    target_ard_product: ArdProduct = field(eq=False, hash=False)

@click.group("ard-bulk-process", help=__doc__)
@ui.environment_option
@ui.config_option
@click.option(
    "--max-count", default=sys.maxsize, help="Maximum number of scenes to process"
)
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
@click.option("--workers-per-node", type=int, default=48, help="Workers per node")
@ui.pass_index(app_name="bulk-reprocess")
@click.pass_context
def cli(ctx, index: AbstractIndex,
        max_count: int,
        workers_per_node: int,
        work_dir: Path,
        pkg_dir: Path):
    ctx.ensure_object(dict)
    ctx.obj['index'] = index
    ctx.obj['max_count'] = max_count
    ctx.obj['workers_per_node'] = workers_per_node
    ctx.obj['work_dir'] = work_dir
    ctx.obj['pkg_dir'] = pkg_dir

@cli.command("search-ards", help="""
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

""")
@click.argument("prefix")
@click.argument("expressions", callback=expression_parse, nargs=-1)
@click.pass_context
def cli_search(ctx, prefix: str, expressions: dict):
    import wagl
    current_wagl_version = wagl.__version__
    structlog_setup()

    log = structlog.get_logger()
    if len(prefix) < 2:
        raise ValueError(
            "Sorry, prefix needs to be at least ls or s2, as we can't process multiple sensors in one go (yet)"
        )

    index = ctx.obj['index']
    max_count = ctx.obj['max_count']
    work_dir = ctx.obj['work_dir']
    workers_per_node = ctx.obj['workers_per_node']
    pkg_dir = ctx.obj['pkg_dir']

    with Datacube(index=index) as dc:
        collection = get_collection(dc, prefix)
        jobs = find_jobs_by_odc_ard_search(dc, collection, expressions, max_count, log)

    platform = prefix[:2]
    create_pbs_jobs(
        jobs=jobs,
        platform=platform,
        work_dir=work_dir,
        workers_per_node=workers_per_node,
        pkg_dir=pkg_dir,
        log=log,
    )


@cli.command("ard-ids", help='Specify a list of ARD UUIDs to reprocess')
# Via a text file
@click.option("-f", "--ids-file", type=click.File("r"), help="File containing UUIDs to process")
@click.argument("ids", nargs=-1)
@click.pass_context
def cli_search(ctx, ids_file, ids: List[str]):
    structlog_setup()

    all_ids = list(ids)
    for id_line in ids_file.readlines():
        all_ids.append(id_line.strip())

    log = structlog.get_logger()

    index = ctx.obj['index']
    max_count = ctx.obj['max_count']
    work_dir = ctx.obj['work_dir']
    workers_per_node = ctx.obj['workers_per_node']
    pkg_dir = ctx.obj['pkg_dir']
    platform = None

    with Datacube(index=index) as dc:
        jobs = []
        for ard_id in all_ids:
            odc_ard = dc.index.datasets.get(ard_id, include_sources=True)
            ard_dataset = ArdDataset.from_odc(odc_ard)
            ard_product = get_product(odc_ard.product.name)

            this_platform = get_platform(odc_ard)
            if platform and (this_platform != platform):
                raise ValueError(f"All IDs should be of the same platform: {platform} != {this_platform}")
            platform = this_platform

            odc_level1:Dataset = odc_ard.sources['level1']
            [level1_product] = [s for s in ard_product.sources if s.name == odc_level1.product.name]

            level1_dataset = Level1Dataset.from_odc(odc_level1, level1_product)
            jobs.append(
                Job(level1_dataset, [ard_dataset.dataset_id], ard_product)
            )

    create_pbs_jobs(
        jobs=jobs,
        platform=platform,
        work_dir=work_dir,
        workers_per_node=workers_per_node,
        pkg_dir=pkg_dir,
        log=log,
    )


def get_platform(odc_ard):
    if odc_ard.metadata.platform.startswith('s'):
        ourplatform = 's2'
    elif odc_ard.metadata.platform.startswith('l'):
        ourplatform = 'ls'
    else:
        raise ValueError(f"Unknown platform: {odc_ard.metadata.platform}")
    return ourplatform


def merge_duplicate_level1s(jobs:List[Job]) -> List[Job]:
    jobs_by_level1_id :Dict[str, List[Job]]= defaultdict(list)
    for job in jobs:
        jobs_by_level1_id[job.level1.dataset_id].append(job)

    for level1_id, job_list in jobs_by_level1_id.items():
        first, *remaining = job_list
        if not remaining:
            yield first
        else:
            yield Job(
                level1=first.level1,
                replacement_uuids=list(chain(*[j.replacement_uuids for j in job_list])),
                target_ard_product=first.target_ard_product,
            )

def create_pbs_jobs(
    jobs: List[Job],
    platform:str,
    work_dir: Path,
    workers_per_node: int,
    pkg_dir: Path,
    log,
):
    if not jobs:
        log.info("no_datasets_to_process")
        return

    # Check for any duplicate level1s, and join the jobs together (just their uuids to archive)
    jobs = list(merge_duplicate_level1s(jobs))

    if platform not in ("ls", "s2"):
        raise ValueError(
            f"Expected collection to begin with either ls or s2, got {platform}"
        )

    environment_file = f"/g/data/v10/work/landsat_downloads/landsat-downloader/config/dass-prod-wagl-{platform}.env"

    job_directory = (
        work_dir / platform / datetime.now().strftime("%Y-%m/%Y-%m-%d-%H%M%S")
    )
    job_directory = job_directory.resolve()
    job_directory.mkdir(parents=True, exist_ok=False)
    scene_list_path = job_directory / "scene-level1-path-list.txt"
    scene_archive_path = job_directory / "scene-archive-list.csv"

    # Write file of level1s to process.
    with scene_list_path.open("w") as fid:
        for level1_job in jobs:
            fid.write(str(level1_job.level1.data_path) + "\n")

    # And a list of ARDs to archive (folder and uuid)?
    with scene_archive_path.open("w") as fid:
        for job in jobs:
            fid.write(f'{job.level1.data_path},{",".join(job.replacement_uuids)}\n')

    dirs = dict(
        workdir=job_directory / "run",
        pkgdir=pkg_dir or job_directory / "pkg",
        # ard-pbs creates subfolders for each batch log anyway.
        logdir=job_directory,
    )
    for _, dir_path in dirs.items():
        dir_path.mkdir(parents=True, exist_ok=True)

    level1_products = list(set(l1.level1.product for l1 in jobs))
    log.info("level1_products", level1_products=[l.name for l in level1_products])
    separate_metadata_dirs = set(
        product.separate_metadata_directory for product in level1_products
    )
    if len(separate_metadata_dirs) > 1:
        raise ValueError(
            f"Level1s have inconsistent separate metadata directory settings: {[p.name for p in level1_products]}"
        )

    if any(separate_metadata_dirs):
        [dirs["yamls-dir"]] = separate_metadata_dirs
        log.info("Using separate metadata directory", directory=dirs["yamls-dir"])

    ard_args = dict(
        project="v10",
        walltime="10:00:00",
        env=environment_file,
        nodes=None,
        workers=workers_per_node,
        **dirs,
    )
    calc_node_with_defaults(ard_args, len(jobs))
    pbs = dedent(
        f"""
        #!/bin/bash

        module purge
        module load pbs

        source {environment_file}

        ard_pbs --level1-list {scene_list_path} {dict_to_cli_args(ard_args, multiline_indent=16)}
    """
    ).lstrip()

    script_path = job_directory / "run_ard_pbs.sh"
    with open(script_path, "w") as src:
        src.write(pbs)
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

    log.info(
        "created_job",
        dataset_count=len(jobs),
        job_directory=str(job_directory),
        script_path=str(script_path),
    )


def find_jobs_by_odc_ard_search(
    dc: Datacube, collection: ArdCollection, expressions: dict, max_count: int, log
) -> List[Job]:
    software_expressions = pop_software_expressions(expressions)

    # The level1s to process, and the ids of datasets that will be replaced by them.
    level1s_to_process: List[Job] = []

    log.info("chosen_products", products=[c.name for c in collection.products])
    # Filter to our set of ARD products.
    for ard_product, ard_dataset in collection.iterate_indexed_ard_datasets(
        expressions
    ):
        ilog = log.bind(dataset_id=ard_dataset.dataset_id)
        if not ard_dataset.metadata_path.exists():
            ilog.warning("dataset_missing_from_disk")
            continue

        if software_expressions:
            if not matches_software_expressions(
                ard_dataset.software_versions(), software_expressions, log=ilog
            ):
                continue

        level1 = dc.index.datasets.get(ard_dataset.level1_id)
        if level1 is None:
            ilog.warning(
                "skip.source_level1_not_indexed", dataset_id=ard_dataset.dataset_id
            )
            # TODO: Perhaps a newer one exists? Or on disk?
            continue

        # TODO: Does a newer Level 1 exist? We'd rather use that.
        level1_product = [
            s for s in ard_product.sources if s.name == level1.product.name
        ][0]
        level1_dataset = Level1Dataset.from_odc(level1, level1_product)
        level1s_to_process.append(Job(
            level1=level1_dataset,
            replacement_uuids=[ard_dataset.dataset_id],
            target_ard_product=ard_product,
        ))

        if len(level1s_to_process) >= max_count:
            log.info("reached_max_dataset_count", max_count=max_count)
            break

    return level1s_to_process


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
    return {k: expressions.pop(k) for k in list(expressions) if k.endswith("_version")}


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
        match value:
            case Range():
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
            case GreaterThan():
                if dataset_version <= version.parse(value.value):
                    log.debug(
                        "skip.software_version_too_low",
                        key=key,
                        expected=value,
                        actual=software_versions[key],
                    )
                    return False
            case LessThan():
                if dataset_version >= version.parse(value.value):
                    log.debug(
                        "skip.software_version_too_high",
                        key=key,
                        expected=value,
                        actual=software_versions[key],
                    )
                    return False
            case _:
                if dataset_version != version.parse(value):
                    log.debug(
                        "skip.software_version_mismatch",
                        key=key,
                        expected=value,
                        actual=software_versions[key],
                    )
                    return False
    return True


if __name__ == "__main__":
    cli()
