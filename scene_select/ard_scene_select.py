#!/usr/bin/env python3

import datetime
import json
import os
import re
from pathlib import Path
from typing import List, Tuple, Dict
from typing import TypedDict

import click
import structlog
from datacube import Datacube
from datacube.index.abstract import AbstractIndex
from datacube.model import Dataset
from datacube.ui import click as ui
from eodatasets3.utils import default_utc

from scene_select import utils, collections
from scene_select.check_ancillary import AncillaryFiles
from scene_select.collections import PROCESSING_PATTERN_MAPPING, get_collection
from scene_select.do_ard import generate_ard_job, ODC_FILTERED_FILE, ArdParameters
from scene_select.library import ArdProduct

HARD_SCENE_LIMIT = 10000

_LOG = structlog.get_logger()


def load_aoi(file_name: str) -> Dict[str, set[str]]:
    """
    load the expected set of region codes for each sat_key ("ls" and "s2")
    """

    with open(file_name, "r") as f:
        data = json.load(f)

    # json does not save sets
    # So after loading the list is converted to a set
    for key, value in data.items():
        data[key] = set(value)
    return data


def does_have_a_mature_child(index: AbstractIndex, dataset: Dataset) -> bool:
    """
    If any child exists that isn't archived, with a dataset_maturity of 'final'
    """
    for child_dataset in index.datasets.get_derived(dataset.id):
        if (
            not child_dataset.is_archived
            and child_dataset.metadata.dataset_maturity == "final"
        ):
            return True
    return False


class SceneInfo(TypedDict):
    dataset_maturity: str
    id: str


ChoppedSceneId = str


def create_table_of_processed_ards(
    index: AbstractIndex, ard_product: ArdProduct
) -> Dict[ChoppedSceneId, SceneInfo]:
    """
    Return None or
    a dictionary with key chopped_scene_id and value id, maturity level.
    """
    sat_key = ard_product.constellation_code

    processed_ard_scene_ids = {}
    if sat_key == "ls":
        scene_id = "landsat_scene_id"
    elif sat_key == "s2":
        scene_id = "sentinel_tile_id"
    else:
        raise ValueError(f"Unknown satellite key {sat_key}")

    for scene_id, dataset_maturity, dataset_uuid in index.datasets.search_returning(
        (scene_id, "dataset_maturity", "id"),
        product=ard_product.name,
    ):
        chopped_id = utils.chopped_scene_id(scene_id)
        if chopped_id in processed_ard_scene_ids:
            # The same chopped scene id has multiple scenes
            old_uuid = processed_ard_scene_ids[chopped_id]["id"]
            _LOG.warning(
                "duplicate_ards",
                landsat_scene_id=chopped_id,
                old_uuid=old_uuid,
                new_uuid=dataset_uuid,
            )
        processed_ard_scene_ids[chopped_id] = SceneInfo(
            id=dataset_uuid, dataset_maturity=dataset_maturity
        )

    return processed_ard_scene_ids


def should_we_filter_due_to_day_excluded(
    days_to_exclude: List, checkdatetime: datetime
):
    """
    days_to_exclude format example;
    '["2020-08-09:2020-08-30", "2020-09-02:2020-09-05"]'
    """
    for period in days_to_exclude:
        start, end = period.split(":")
        # datetime.timezone.utc
        # pytz.UTC
        start = datetime.datetime.strptime(start, "%Y-%m-%d").replace(
            tzinfo=checkdatetime.tzinfo
        )
        end = datetime.datetime.strptime(end, "%Y-%m-%d").replace(
            tzinfo=checkdatetime.tzinfo
        )

        # let's make it the end of the day
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        if start <= checkdatetime <= end:
            return True
    return False


def should_we_filter_due_to_ancil(
    l1_dataset: Dataset,
    ancill_there: bool,
    msg: str,
    interim_days_wait: int,
    temp_logger,
) -> bool:
    if ancill_there:
        return False
    days_ago = datetime.datetime.now(l1_dataset.time.end.tzinfo) - datetime.timedelta(
        days=interim_days_wait
    )
    if days_ago > l1_dataset.time.end:
        # If the ancillary files take too long to turn up
        # process anyway
        temp_logger.debug(
            "processing.too_old",
            message=f"No ancil, but the scene is too old to keep waiting: {msg}",
            days_ago=str(days_ago),
            dataset_time_end=str(l1_dataset.time.end),
        )
        return False
    else:
        temp_logger.info(
            "filtering.no_ancil_yet",
            reason="ancillary files not ready",
            days_ago=str(days_ago),
            dataset_time_end=str(l1_dataset.time.end),
            message=f"Not ready: {msg}",
        )
        return True


def should_we_filter_because_processed_already(
    index: AbstractIndex,
    l1_dataset: Dataset,
    processed_ard_scene_ids: dict[str, dict],
    find_blocked: bool,
    ancill_there: bool,
    uuids2archive: list[str],
    choppedsceneid: str,
    temp_logger,
):
    # Do the data with child filter here
    # It will slow things down
    # But any chopped_scene_id in processed_ard_scene_ids
    # will now be a blocked reprocessed scene
    if find_blocked:
        if does_have_a_mature_child(index, l1_dataset):
            temp_logger.debug("filtering", reason="Skipping dataset with children")
            return True

    if choppedsceneid not in processed_ard_scene_ids:
        return False

    kwargs = {}
    produced_ard = processed_ard_scene_ids[choppedsceneid]
    if find_blocked:
        kwargs["reason"] = "Potential blocked reprocessed scene."
        kwargs["blocking_ard_scene_id"] = str(produced_ard["id"])
        # Since all dataset with final childs
        # have been filtered out
    else:
        kwargs["reason"] = "The scene has been processed"
        # Since dataset with final childs have not been
        # filtered out we don't know why there is
        # an ard there.

    if produced_ard["dataset_maturity"] == "interim" and ancill_there is True:
        uuids2archive.append(str(produced_ard["id"]))
        temp_logger.debug(
            "scene added", reason="Interim scene is being processed to final"
        )
        return False

    temp_logger.debug("filtering.already_in_list", **kwargs)
    return True


def find_to_process(
    collection: collections.ArdCollection,
    ancil_config: AncillaryFiles,
    region_codes_per_sat_key: Dict[str, set[str]],
    interim_days_wait: int,
    days_to_exclude: List,
    find_blocked: bool,
    min_date: datetime.datetime,
    max_date: datetime.datetime,
    only_active_products=True,
) -> Tuple[list[str], list[str], int]:
    sat_key = collection.constellation_code

    files2process = set({})
    duplicates = 0
    uuids2archive = []
    index = collection.dc.index

    for ard_product in collection.ard_products:
        for level1_product in ard_product.sources:
            if only_active_products and not level1_product.is_active:
                continue

            l1_product_name = level1_product.name
            # This is used to block reprocessing of reprocessed l1's
            processed_ard_scene_ids = create_table_of_processed_ards(
                index, l1_product_name
            )

            # Don't crash on unknown l1 products
            if l1_product_name not in PROCESSING_PATTERN_MAPPING:
                msg = " not known to scene select processing filtering. Disabling processing filtering."
                _LOG.warn(l1_product_name + msg)

            product_start_time, product_end_time = (
                index.datasets.get_product_time_bounds(product=l1_product_name)
            )

            if min_date:
                product_start_time = max(product_start_time, min_date)
            if max_date:
                product_end_time = min(product_end_time, max_date)

            # Query month-by-month to make DB queries smaller.
            # Note that we may receive the same dataset multiple times due to boundaries (hence: results as a set)
            for year, month in utils.iterate_months(
                product_start_time, product_end_time
            ):
                for l1_dataset in index.datasets.search(
                    product=l1_product_name, time=utils.month_as_range(year, month)
                ):
                    if sat_key == "ls":
                        product_id = l1_dataset.metadata.landsat_product_id
                        choppedsceneid = utils.chopped_scene_id(
                            l1_dataset.metadata.landsat_scene_id
                        )
                    elif sat_key == "s2":
                        product_id = l1_dataset.metadata.sentinel_tile_id
                        choppedsceneid = utils.chopped_scene_id(
                            l1_dataset.metadata.sentinel_tile_id
                        )
                    else:
                        raise ValueError("Unknown satellite key")

                    region_code = l1_dataset.metadata.region_code
                    file_path = utils.calc_file_path(l1_dataset, product_id)

                    log = _LOG.bind(
                        landsat_scene_id=product_id,
                        dataset_id=str(l1_dataset.id),
                        dataset_path=file_path,
                    )

                    # Filter out if the processing level is too low
                    if l1_product_name in PROCESSING_PATTERN_MAPPING:
                        prod_pattern = PROCESSING_PATTERN_MAPPING[l1_product_name]
                        if not re.match(prod_pattern, product_id):
                            log.debug(
                                "filtering.low_processing_level",
                                reason="Processing level too low",
                            )
                            continue

                    # Filter out if outside area of interest
                    if (
                        sat_key is not None
                        and region_code not in region_codes_per_sat_key[sat_key]
                    ):
                        log.debug(
                            "filtering.outside_aoi",
                            reason="Region not in AOI",
                            region_code=region_code,
                        )
                        continue

                    ancill_there, msg = ancil_config.is_ancil_there(l1_dataset.time.end)

                    # Continue here if a maturity level of final cannot be produced
                    # since the ancillary files are not there
                    if should_we_filter_due_to_ancil(
                        l1_dataset, ancill_there, msg, interim_days_wait, log
                    ):
                        continue

                    # FIXME remove the hard-coded list
                    if should_we_filter_due_to_day_excluded(
                        days_to_exclude, l1_dataset.time.end
                    ):
                        log.info(
                            "filtering.excluded_day",
                            dataset_time_end=l1_dataset.time.end,
                            reason="This day is excluded.",
                        )
                        continue

                    # Filter out duplicate zips
                    if file_path in files2process:
                        duplicates += 1
                        log.debug(
                            "filtering.multi_file_path",
                            reason="Potential multi-granule duplicate file path removed.",
                            duplicate_count=duplicates,
                        )
                        continue

                    if should_we_filter_because_processed_already(
                        index,
                        l1_dataset,
                        processed_ard_scene_ids,
                        find_blocked,
                        ancill_there,
                        uuids2archive,
                        choppedsceneid,
                        log,
                    ):
                        continue

                    # WARNING any filter under here will
                    # be executed on interim scenes that it is assumed will
                    # be processed

                    # LOGGER.debug("location:pre dataset_with_final_child")
                    # If any child exists that isn't archived
                    if does_have_a_mature_child(index, l1_dataset):
                        log.debug(
                            "filtering.has_mature_children",
                            reason="Skipping dataset with children",
                        )
                        continue

                    files2process.add(file_path)

    return list(files2process), uuids2archive, duplicates


def _get_path_date(path: str) -> str:
    """
    >>> _get_path_date('/g/data/da82/AODH/USGS/L1/Landsat/C2/135_097/LC81350972022337/LC08_L1GT_135097_20221203_20221212_02_T2.tar')
    '20221203'
    >>> _get_path_date('/g/data/da82/AODH/USGS/L1/Landsat/C2/135_097/LC91350972023268/LC09_L1GT_135097_20230925_20230925_02_T2.tar')
    '20230925'
    >>> _get_path_date('/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2022/2022-07/05S140E-10S145E/S2A_MSIL1C_20220706T005721_N0400_R002_T54LWQ_20220706T022422.zip')
    '20220706T005721'
    """
    filename = os.path.basename(path)
    try:
        if filename.upper().startswith("L"):
            return filename.split("_")[3]
        elif filename.upper().startswith("S"):
            return filename.split("_")[2]
    except IndexError:
        pass

    # If the filename doesn't follow that pattern, just sort it last
    return "00000000"


@click.command()
@ui.environment_option
@ui.config_option
@click.argument("prefix")
@click.option(
    "--workdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output working directory.",
    default=Path.cwd(),
)
@click.option(
    "--scene-limit",
    default=1000,
    type=int,
    help="Safety limit: Maximum number of scenes to process in a run. "
    "multigranule zip files may exceed this limit.",
)
@click.option(
    "--interim-days-wait",
    default=23,
    type=int,
    help="Maxi days to wait for ancillary data before processing ARD to "
    "an interim maturity level.",
)
@click.option(
    "--days-to-exclude",
    cls=utils.PythonLiteralOption,
    type=list,
    help="List of ranges of dates to not process, "
    "as (start date: end date) with format (yyyy-mm-dd:yyyy-mm-dd). e.g. "
    "'2019-12-22:2019-12-25,2020-08-09:2020-09-03'",
    default=[],
)
@click.option(
    "--run-ard",
    default=False,
    is_flag=True,
    help="Submit these as ARD jobs to PBS.",
)
# These are passed on to ard processing
@click.option(
    "--test",
    default=False,
    is_flag=True,
    help="Test job execution (Run `ard-pbs` but tell it not to submit the job).",
)
@click.option(
    "--find-blocked",
    default=False,
    is_flag=True,
    help="Find l1 scenes with no children that are not getting processed.",
)
@utils.LogAnyErrors(_LOG)
@ui.pass_index(app_name="scene-select")
def scene_select(
    index: AbstractIndex,
    allowed_codes: str,
    prefix: str,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: list,
    run_ard: bool,
    find_blocked: bool,
):
    only_active_products = True
    with Datacube(index=index) as dc:
        collection = get_collection(dc, prefix)

    constellation_code = collection.constellation_code

    run_date = datetime.datetime.now()
    jobdir = Path(
        f"/g/data/v10/work/{constellation_code}-submit-ard/{run_date:%Y-%m}/{run_date:%d-%H%M%S}"
    )
    scene_select_jobdir = jobdir / "scene-select"
    logdir = Path(
        f"/g/data/v10/logs/{constellation_code}-submit-ard/{run_date:%Y-%m}/{run_date:%d-%H%M%S}"
    )
    jobdir.mkdir(exist_ok=True, parents=True)
    logdir.mkdir(exist_ok=True, parents=True)

    # Create the ArdClickParameters instance with calculated paths
    ard_params = ArdParameters(
        walltime="10:00:00",
        project="v10",
        logdir=logdir.as_posix(),
        jobdir=jobdir.as_posix(),
        pkgdir="/g/data/xu18/ga",
        env=f"/g/data/v10/work/landsat_downloads/landsat-downloader/config/dass-prod-wagl-{constellation_code}.env",
        index_datacube_env="/g/data/v10/work/landsat_downloads/landsat-downloader/config/dass-index-datacube.env",
    )

    # This was on the old scene-select airflow. Not sure why.
    if constellation_code == "s2":
        days_to_exclude.append("2015-01-01:2022-08-18")

    default_max_date = default_utc(datetime.datetime.now(datetime.UTC))
    default_min_date = default_max_date - datetime.timedelta(days=60)

    gen_log_file = logdir.joinpath("ard_scene_select.jsonl").resolve()
    utils.structlog_setup(gen_log_file.open("a"))

    _LOG.info("scene_select", **locals())

    ancil_config = collections.ANCILLARY_COLLECTION

    scene_limit = min(scene_limit, HARD_SCENE_LIMIT)

    files2process, uuids2archive, duplicates = find_to_process(
        collection=collection,
        ancil_config=ancil_config,
        region_codes_per_sat_key=load_aoi(allowed_codes),
        interim_days_wait=interim_days_wait,
        days_to_exclude=days_to_exclude,
        find_blocked=find_blocked,
        min_date=default_min_date,
        max_date=default_max_date,
    )

    # If we stopped above as soon as we reached the limit we could end up in a situation where
    # only the first product is ever processed.

    # Sort files so most recent acquisitions are processed first.
    # This is to avoid a backlog holding up recent acquisitions
    files2process.sort(key=_get_path_date, reverse=True)

    # TODO: the old code reduced written records by the duplicate count, seemingly for multi-granule to be counted
    #       multiple times. But it also had a comment saying it wouldn't work well with multi-granule...
    l1_count = min(len(files2process), scene_limit)

    level1_list_file = jobdir.joinpath(ODC_FILTERED_FILE)
    with level1_list_file.open("w") as fid:
        for path in files2process[:l1_count]:
            fid.write(str(path) + "\n")

    # Check for separate yaml dirs in our products
    yaml_dirs = set(
        [
            l1_product.separate_metadata_directory
            for l1_product in collection.iter_level1_products()
        ]
    )
    yaml_dirs.remove(None)
    if yaml_dirs:
        if len(yaml_dirs) > 1:
            raise ValueError(
                "Multiple yaml directories found. For our given constellation?"
            )
        ard_params["yamls_dir"] = yaml_dirs.pop().as_posix()

    generate_ard_job(
        ard_params,
        l1_count,
        level1_list_file,
        uuids2archive,
        jobdir,
        run_ard,
    )

    _LOG.info("info", jobdir=str(jobdir))


if __name__ == "__main__":
    scene_select()
