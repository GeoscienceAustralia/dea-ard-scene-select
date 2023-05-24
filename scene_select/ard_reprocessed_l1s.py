#!/usr/bin/env python3
"""
THIS IS LANDSAT ONLY
"""

from pathlib import Path
from logging.config import fileConfig
import click
from datetime import timedelta, datetime
import pprint
import uuid

from scene_select.dass_logs import LOGGER, LogMainFunction
from scene_select import utils
from scene_select.do_ard import do_ard

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.model import Range

PRODUCT = "ga_ls9c_ard_3"
DIR_TEMPLATE = "reprocess-jobid-{jobid}"
LOG_FILE = "reprocessing.log"
THIS_TASK = "archive_and_move_for_reprocessing"


class PathPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def landsat_date(product_id):
    # Extract date string from the product ID
    date_str = product_id.split("_")[3][0:8]

    # Convert date string to datetime object
    date_obj = datetime.strptime(date_str, "%Y%m%d")

    return date_obj


def find_blocked_l1_for_a_dataset(dc, dataset):
    """
    Find the blocked l1 for a given dataset.

    return None or a list with one dataset.
    """
    blocked_l1s = []
    blocking_scene_id = dataset.metadata.landsat_scene_id
    blocking_product_id = dataset.metadata.landsat_product_id
    previous_dataset_versions = dc.index.datasets.search_eager(
        product_family="level1",
        platform=dataset.metadata.platform,
        region_code=dataset.metadata.region_code,
        time=Range(
            dataset.time.begin - timedelta(days=1),
            dataset.time.end + timedelta(days=1),
        ),
    )
    for previous_dataset in previous_dataset_versions:
        previous_scene_id = previous_dataset.metadata.landsat_scene_id
        if previous_dataset.id == dataset.id:
            # Skip the current dataset
            continue
        # assert the chopped scenes are the same
        if not utils.chopped_scene_id(previous_scene_id) == utils.chopped_scene_id(
            blocking_scene_id
        ):
            LOGGER.info(
                "skipped l1 pairs with different chopped scene ids",
                blocking_scene_id=blocking_scene_id,
                other_l1_id=previous_dataset.id,
                blocking_l1_ds=dataset,
            )
            continue

        # check that the blocked scene has a later processing date
        previous_product_id = previous_dataset.metadata.landsat_product_id
        previous_date = landsat_date(previous_product_id)
        blocking_date = landsat_date(blocking_product_id)
        if previous_date < blocking_date:
            LOGGER.info(
                "skipped l1 pairs with blocked processing date less than blocking date",
                blocking_l1_id=blocking_scene_id,
                blocked_l1_id=previous_dataset.id,
                blocking_l1_ds=dataset,
            )
            continue

        LOGGER.info(
            "l1 pairs",
            blocking_scene_id=blocking_scene_id,
            blocked_l1_id=previous_dataset.id,
            blocking_l1_ds=dataset,
        )

        blocked_l1s.append(previous_dataset)
    # Two or more blocked l1s is a problem
    if len(blocked_l1s) > 1:
        LOGGER.error(
            "multiple blocked l1s. Ignore this group of l1s", dataset_id=dataset.id,
        )
        blocked_l1s = []
    return blocked_l1s


def find_blocked(dc, product, scene_limit):
    blocked_scenes = []
    for tmp_dataset in dc.index.datasets.search_returning(("id",), product=product):
        ard_id = tmp_dataset.id
        ard_dataset = dc.index.datasets.get(ard_id, include_sources=True)

        l1_id = ard_dataset.metadata_doc["lineage"]["source_datasets"]["level1"]["id"]
        l1_ds = dc.index.datasets.get(l1_id)

        if l1_ds.is_archived:
            # All blocking l1s are archived.
            # l1s are archived for other reasons too though.
            # Check if there is a blocked l1
            # LOGGER.info("ARD with archived l1", blocking_l1=blocking_l1_id, archive=ard_id)
            blocked_l1 = find_blocked_l1_for_a_dataset(dc, l1_ds)
            # blocked_l1 is None or a list with one dataset.
            if blocked_l1 is None:
                # Could not find an l1 that is being blocked.
                continue
            # this is the yaml file
            blocked_l1_local_path = blocked_l1[0].local_path
            blocked_l1_zip_path = utils.calc_file_path(
                blocked_l1[0], blocked_l1[0].metadata.landsat_product_id
            )
            blocking_ard_zip_path = utils.calc_file_path(
                ard_dataset, ard_dataset.metadata.landsat_product_id
            )
            # pprint.pprint (blocked_l1[0].metadata_doc)
            LOGGER.info(
                "Found_blocked_l1",
                blocked_l1_zip_path=blocked_l1_zip_path,
                archive=str(ard_id),
            )
            blocked_scenes.append(
                {
                    "blocking_ard_id": str(ard_id),
                    "blocked_l1_zip_path": blocked_l1_zip_path,
                    "blocking_ard_zip_path": blocking_ard_zip_path,
                }
            )
        if len(blocked_scenes) >= scene_limit:
            LOGGER.info(
                "scene_limit reached",
                len_blocked_scenes=str(len(blocked_scenes)),
                scene_limit=str(scene_limit),
            )
            break
    return blocked_scenes


def move_blocked(
    blocked_scenes: dict,
    current_base_path: click.Path,
    new_base_path: click.Path,
    dry_run: bool,
):
    l1_zips = []
    uuids2archive = []
    if len(blocked_scenes) > 0:
        # move the blocked scenes
        for scene in blocked_scenes:
            # move the blocking ARD
            if dry_run:
                worked = True
                status = None
                outs = None
                errs = None
            else:
                worked, status, outs, errs = utils.scene_move(
                    Path(scene["blocking_ard_zip_path"]),
                    current_base_path,
                    new_base_path,
                )
            if worked:
                l1_zips.append(scene["blocked_l1_zip_path"])
                uuids2archive.append(scene["blocking_ard_id"])

                LOGGER.info(
                    "To reprocess",
                    blocking_ard_zip_path=scene["blocking_ard_zip_path"],
                    blocked_l1_zip_path=scene["blocked_l1_zip_path"],
                    blocking_ard_id=scene["blocking_ard_id"],
                )
    return l1_zips, uuids2archive


@click.command()
@click.option(
    "--config",
    type=PathPath(dir_okay=False, file_okay=True),
    help="Full path to a datacube config text file."
    " This describes the ODC database.",
    default=None,
)
@click.option(
    "--current-base-path",
    help="base path of the current ARD product. e.g. /g/data/xu18/ga",
    default="/g/data/xu18/ga",
    type=PathPath(exists=True),
)
@click.option(
    "--new-base-path",
    help="Move datasets here before deleting them. e.g. /g/data/xu18/ga/reprocessing_staged_for_removal",
    default="/g/data/xu18/ga/reprocessing_staged_for_removal",
    type=PathPath(exists=True),
)
@click.option(
    "--product",
    help="The ODC product to be reprocessed. e.g. ga_ls9c_ard_3",
    default=PRODUCT,
)
@click.option(
    "--workdir",
    type=PathPath(file_okay=False, writable=True),
    help="The base output working directory.",
    default=Path.cwd(),
)
@click.option(
    "--scene-limit",
    default=1000,
    type=int,
    help="Safety limit: Maximum number of scenes to process in a run. \
Does not work for multigranule zip files.",
)
@click.option(
    "--run-ard",
    default=False,
    is_flag=True,
    help="Produce ARD scenes by executing the ard_pbs script.",
)
# These are passed on to ard processing
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Do not actually archive or move scenes.",
)
@click.option(
    "--log-config",
    type=PathPath(dir_okay=False, file_okay=True, exists=True),
    default=utils.LOG_CONFIG,
    help="full path to the logging configuration file",
)
@click.option("--stop-logging", default=False, is_flag=True, help="No logs.")
@click.option("--walltime", help="Job walltime in `hh:mm:ss` format.")
@click.option("--email", help="Notification email address.")
@click.option("--project", default="v10", help="Project code to run under.")
@click.option(
    "--logdir",
    type=PathPath(file_okay=False, writable=True),
    help="The base logging and scripts output directory.",
)
@click.option(
    "--jobdir",
    type=PathPath(file_okay=False, writable=True),
    help="The start ard processing directory. Will be made if it does not exist.",
)
@click.option(
    "--pkgdir",
    type=PathPath(file_okay=False, writable=True),
    help="The base output packaged directory.",
)
@click.option(
    "--env",
    type=PathPath(exists=True, readable=True),
    help="Environment script to source for ard_pipelines.",
)
@click.option(
    "--index-datacube-env",
    type=PathPath(exists=True, readable=True),
    help="Path to the datacube indexing environment. "
    "Add this to index the ARD results.  "
    "If this option is not defined the ARD results "
    "will not be automatically indexed.",
)
@click.option(
    "--workers",
    type=click.IntRange(1, 48),
    help="The number of workers to request per node.",
)
@click.option("--nodes", help="The number of nodes to request.")
@click.option("--memory", help="The memory in GB to request per node.")
@click.option("--jobfs", help="The jobfs memory in GB to request per node.")
@LogMainFunction()
def ard_reprocessed_l1s(
    config: Path,
    current_base_path: Path,
    new_base_path: Path,
    product: list,
    jobdir: Path,
    logdir: Path,
    stop_logging: bool,
    log_config: Path,
    scene_limit: int,
    run_ard: bool,
    dry_run: bool,
    **ard_click_params: dict,
):
    """
    The keys for ard_click_params;
        workdir: click.Path,
        pkgdir: click.Path,
        env: click.Path,
        workers: int,
        nodes: int,
        memory: int,
        jobfs: int,
        project: str,
        walltime: str,
        email: str

    :return: list of scenes to ARD process
    """
    # pylint: disable=R0913, R0914
    # R0913: Too many arguments
    # R0914: Too many local variables

    logdir = Path(logdir).resolve()
    # If we write a file we write it in the job dir
    # set up the scene select job dir in the log dir
    if jobdir is None:
        logdir = Path(logdir).resolve()
        jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    if not stop_logging:
        gen_log_file = jobdir.joinpath(LOG_FILE).resolve()
        fileConfig(
            log_config,
            disable_existing_loggers=False,
            defaults={"genlogfilename": str(gen_log_file)},
        )

    # logdir is used both  by scene select and ard
    # So put it in the ard parameter dictionary
    ard_click_params["logdir"] = logdir

    LOGGER.info("reprocessed_l1s", **locals())
    dc = datacube.Datacube(app=THIS_TASK)

    # identify the blocking ARD uuids and locations
    blocked_scenes = find_blocked(dc, product, scene_limit)

    l1_zips, uuids2archive = move_blocked(
        blocked_scenes, current_base_path, new_base_path, dry_run
    )
    l1_count = len(l1_zips)
    usgs_level1_files = None
    do_ard(
        ard_click_params,
        l1_count,
        usgs_level1_files,
        uuids2archive,
        jobdir,
        run_ard,
        l1_zips,
    )

    return jobdir


if __name__ == "__main__":
    ard_reprocessed_l1s()
