#!/usr/bin/env python3

import datetime
import json
import os
import re
import uuid
from logging.config import fileConfig
from pathlib import Path
from typing import List, Tuple, Dict, TypedDict

import click
import structlog
from datacube import Datacube
from datacube.model import Dataset
from eodatasets3.utils import default_utc

from scene_select.check_ancillary import AncillaryFiles
from scene_select.do_ard import generate_ard_job, ODC_FILTERED_FILE
from scene_select import utils, collections

AOI_FILE = "Australian_AOI.json"
# AOI_FILE = "Australian_AOI_mainland.json"
# AOI_FILE = "Australian_AOI_with_islands.json"


HARD_SCENE_LIMIT = 10000

_LOG = structlog.get_logger()


def _make_patterns():
    L9_C2_PATTERN = (
        r"^(?P<sensor>LC)"
        r"(?P<satellite>09)_"
        r"(?P<processingCorrectionLevel>L1TP|L1GT)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>02)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

    # landsat 8 filename pattern is configured to match only
    # processing level L1TP and L1GT for acquisition containing
    # both the TIRS and OLI sensors with .tar extension.
    L8_C1_PATTERN = (
        r"^(?P<sensor>LC)"
        r"(?P<satellite>08)_"
        r"(?P<processingCorrectionLevel>L1TP|L1GT)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>01)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

    L8_C2_PATTERN = (
        r"^(?P<sensor>LC)"
        r"(?P<satellite>08)_"
        r"(?P<processingCorrectionLevel>L1TP|L1GT)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>02)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )
    # L1TP and L1GT are all ortho-rectified with DEM.
    # The only difference is L1GT was processed without Ground Control Points
    # - but because LS8 orbit is very accurate so LS8 L1GT products with orbital
    # info is ~90% within one pixel.
    # (From Lan-Wei)
    # Therefore we use L1GT for ls8 but not ls7 or ls5.

    # landsat 7 filename pattern is configured to match only
    # processing level L1TP with .tar extension.
    L7_C1_PATTERN = (
        r"^(?P<sensor>LE)"
        r"(?P<satellite>07)_"
        r"(?P<processingCorrectionLevel>L1TP)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>01)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

    L7_C2_PATTERN = (
        r"^(?P<sensor>LE)"
        r"(?P<satellite>07)_"
        r"(?P<processingCorrectionLevel>L1TP)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>02)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

    # landsat 5 filename is configured to match only
    # processing level L1TP with .tar extension.
    L5_PATTERN = (
        r"^(?P<sensor>LT)"
        r"(?P<satellite>05)_"
        r"(?P<processingCorrectionLevel>L1TP)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>01)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

    S2_PATTERN = r"^(?P<satellite>S2)" + r"(?P<satelliteid>[A-C])_"

    return {
        "ga_ls5t_level1_3": L5_PATTERN,
        "ga_ls7e_level1_3": L7_C1_PATTERN,
        "usgs_ls5t_level1_1": L5_PATTERN,
        "usgs_ls7e_level1_1": L7_C1_PATTERN,
        "usgs_ls7e_level1_2": L7_C2_PATTERN,
        "usgs_ls8c_level1_1": L8_C1_PATTERN,
        "usgs_ls8c_level1_2": L8_C2_PATTERN,
        "usgs_ls9c_level1_2": L9_C2_PATTERN,
        "esa_s2am_level1_0": S2_PATTERN,
        "esa_s2bm_level1_0": S2_PATTERN,
        "esa_s2cm_level1_0": S2_PATTERN,
    }


PROCESSING_PATTERN_MAPPING = _make_patterns()


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


def does_have_a_mature_child(dc: Datacube, dataset: Dataset) -> bool:
    """
    If any child exists that isn't archived, with a dataset_maturity of 'final'
    :param dc:
    :param dataset:
    :return:
    """
    for child_dataset in dc.index.datasets.get_derived(dataset.id):
        if (
            not child_dataset.is_archived
            and child_dataset.metadata.dataset_maturity == "final"
        ):
            return True
    return False


class ARDSceneInfo(TypedDict):
    dataset_maturity: str
    id: str


ChoppedSceneId = str


def create_table_of_processed_ards(
    dc: Datacube, product_name: str, sat_key: str
) -> Dict[ChoppedSceneId, ARDSceneInfo]:
    """
    Return None or
    a dictionary with key chopped_scene_id and value id, maturity level.
    """
    ard_product = collections.get_product_for_level1(product_name)
    if not ard_product:
        raise ValueError(f"Product {product_name!r} is not a known ARD product")

    processed_ard_scene_ids = {}
    if sat_key == "ls":
        scene_id = "landsat_scene_id"
    elif sat_key == "s2":
        scene_id = "sentinel_tile_id"
    else:
        raise ValueError(f"Unknown satellite key {sat_key}")

    for result in dc.index.datasets.search_returning(
        (scene_id, "dataset_maturity", "id"),
        product=ard_product.name,
    ):
        chopped_id = utils.chopped_scene_id(result.landsat_scene_id)
        if chopped_id in processed_ard_scene_ids:
            # The same chopped scene id has multiple scenes
            old_uuid = processed_ard_scene_ids[chopped_id]["id"]
            _LOG.warning(
                "Multiple identical ARD scene ids",
                landsat_scene_id=chopped_id,
                old_uuid=old_uuid,
                new_uuid=result.id,
            )
        processed_ard_scene_ids[chopped_id] = {
            "dataset_maturity": result.dataset_maturity,
            "id": result.id,
        }

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


def get_aoi_sat_key(region_codes: Dict, product_name: str):
    aoi_sat_key = None
    for key in region_codes.keys():
        if key in product_name:
            aoi_sat_key = key
            continue
    return aoi_sat_key


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
    dc: Datacube,
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
        if does_have_a_mature_child(dc, l1_dataset):
            temp_logger.debug("filtering", reason="Skipping dataset with children")
            return True

    if choppedsceneid not in (processed_ard_scene_ids or {}):
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
        # lets build a list of ARD uuid's to delete
        uuids2archive.append(str(produced_ard["id"]))

        temp_logger.debug(
            "scene added", reason="Interim scene is being processed to final"
        )
        return False
    else:
        temp_logger.debug("filtering", **kwargs)
        # Continue for everything except interim
        # so it doesn't get processed
        return True


def find_to_process(
    dc: Datacube,
    l1_product_name: str,
    ancil_config: AncillaryFiles,
    region_codes_per_sat_key: Dict[str, set[str]],
    interim_days_wait: int,
    days_to_exclude: List,
    find_blocked: bool,
    min_date: datetime.datetime,
    max_date: datetime.datetime,
) -> Tuple[list[str], list[str], int]:
    sat_key = get_aoi_sat_key(region_codes_per_sat_key, l1_product_name)

    # This is used to block reprocessing of reprocessed l1's
    processed_ard_scene_ids = create_table_of_processed_ards(
        dc, l1_product_name, sat_key
    )

    # Don't crash on unknown l1 products
    if l1_product_name not in PROCESSING_PATTERN_MAPPING:
        msg = " not known to scene select processing filtering. Disabling processing filtering."
        _LOG.warn(l1_product_name + msg)

    files2process = set({})
    duplicates = 0
    uuids2archive = []
    product_start_time, product_end_time = dc.index.datasets.get_product_time_bounds(
        product=l1_product_name
    )

    if min_date:
        product_start_time = max(product_start_time, min_date)
    if max_date:
        product_end_time = min(product_end_time, max_date)

    # Query month-by-month to make DB queries smaller.
    # Note that we may receive the same dataset multiple times due to boundaries (hence: results as a set)
    for year, month in utils.iterate_months(product_start_time, product_end_time):
        for l1_dataset in dc.index.datasets.search(
            product=l1_product_name, time=utils.month_as_range(year, month)
        ):
            if sat_key == "ls":
                product_id = l1_dataset.metadata.landsat_product_id
                choppedsceneid = utils.chopped_scene_id(
                    l1_dataset.metadata.landsat_scene_id
                )
            elif sat_key == "s2":
                product_id = l1_dataset.metadata.sentinel_tile_id
                # S2 has no equivalent to a scene id
                # I'm using sentinel_tile_id.  This will work for handling interim to final.
                # it will not catch duplicates.
                choppedsceneid = l1_dataset.metadata.sentinel_tile_id
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
                    log.debug("filtering", reason="Processing level too low")
                    continue

            # Filter out if outside area of interest
            if (
                sat_key is not None
                and region_code not in region_codes_per_sat_key[sat_key]
            ):
                log.debug(
                    "filtering", reason="Region not in AOI", region_code=region_code
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
                    "filtering",
                    dataset_time_end=l1_dataset.time.end,
                    reason="This day is excluded.",
                )
                continue

            # Filter out duplicate zips
            if file_path in files2process:
                duplicates += 1
                log.debug(
                    "filtering",
                    reason="Potential multi-granule duplicate file path removed.",
                    duplicate_count=duplicates,
                )
                continue

            if should_we_filter_because_processed_already(
                dc,
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
            if does_have_a_mature_child(dc, l1_dataset):
                log.debug("filtering", reason="Skipping dataset with children")
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
@click.option(
    "--usgs-level1-files",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a text file containing all "
    "the level-1 USGS/ESA entries to be filtered",
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=utils.DATA_DIR.joinpath(AOI_FILE),
    help="full path to a json file containing path/row and "
    "MGRS tiles to act as a area of interest filter",
)
@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="Full path to a datacube config text file. This describes the ODC database.",
    default=None,
)
@click.option(
    "--products",
    cls=utils.PythonLiteralOption,
    type=list,
    help="List the ODC L1 products to be processed. e.g."
    ' \'["ga_ls5t_level1_3", "usgs_ls8c_level1_1"]\'',
    default='["usgs_ls8c_level1_2", "usgs_ls9c_level1_2"]',
)
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
    default=40,
    type=int,
    help="Maxi days to wait for ancillary data before processing ARD to "
    "an interim maturity level.",
)
@click.option(
    "--days-to-exclude",
    cls=utils.PythonLiteralOption,
    type=list,
    help="List of ranges of dates to not process, "
    "as (start date: end date) with format (yyyy-mm-dd:yyyy-mm-dd). e.g."
    ' \'["2019-12-22:2019-12-25", "2020-08-09:2020-09-03"]\'',
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
    "--log-config",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=utils.DATA_DIR.joinpath(utils.LOG_CONFIG_FILE),
    help="full path to a logging configuration file",
)
@click.option(
    "--yamls-dir",
    type=click.Path(file_okay=False),
    default="",
    help="The base directory for level-1 dataset documents.",
)
@click.option("--stop-logging", default=False, is_flag=True, help="No logs.")
@click.option("--walltime", help="Job walltime in `hh:mm:ss` format.")
@click.option("--email", help="Notification email address.")
@click.option("--project", default="v10", help="Project code to run under.")
@click.option(
    "--logdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base logging and scripts output directory.",
)
@click.option(
    "--jobdir",
    type=click.Path(file_okay=False, writable=True),
    help="The start ard processing directory. Will be made if it does not exist.",
)
@click.option(
    "--pkgdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output packaged directory.",
)
@click.option(
    "--env",
    type=click.Path(exists=True, readable=True),
    help="Environment script to source.",
)
@click.option(
    "--index-datacube-env",
    type=click.Path(exists=True, readable=True),
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
@click.option(
    "--find-blocked",
    default=False,
    is_flag=True,
    help="Find l1 scenes with no children that are not getting processed.",
)
@utils.LogAnyErrors(_LOG)
def scene_select(
    usgs_level1_files: str,
    allowed_codes: str,
    config: str,
    products: list,
    logdir: str,
    jobdir: str,
    stop_logging: bool,
    log_config: str,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: list,
    run_ard: bool,
    find_blocked: bool,
    **ard_click_params,
):
    utils.structlog_setup()

    default_max_date = default_utc(datetime.datetime.now(datetime.UTC))
    default_min_date = default_max_date - datetime.timedelta(days=60)

    logdir = Path(logdir).resolve()

    # If we write a file we write it in the job dir
    # set up the scene select job dir in the log dir
    if jobdir is None:
        jobdir = logdir.joinpath(f"filter-jobid-{uuid.uuid4().hex[0:6]}")
    else:
        jobdir = Path(jobdir).resolve()
    jobdir.mkdir(exist_ok=True)

    if not stop_logging:
        gen_log_file = jobdir.joinpath("ard_scene_select.log").resolve()
        fileConfig(
            log_config,
            disable_existing_loggers=False,
            defaults={"genlogfilename": str(gen_log_file)},
        )
    _LOG.info("scene_select", **locals())

    # logdir is used both  by scene select and ard
    # So put it in the ard parameter dictionary
    ard_click_params["logdir"] = logdir

    if usgs_level1_files:
        uuids2archive = []
        l1_count = sum(1 for _ in open(usgs_level1_files))
        generate_ard_job(
            ard_click_params,
            l1_count,
            Path(usgs_level1_files).resolve(),
            uuids2archive,
            jobdir,
            run_ard,
        )
    else:
        usgs_level1_files = jobdir.joinpath(ODC_FILTERED_FILE)
        duplicate_count = 0
        uuids2archive_combined = []
        paths_to_process = []
        ancil_config = AncillaryFiles()

        scene_limit = min(scene_limit, HARD_SCENE_LIMIT)
        with Datacube(app="ard-scene-select", config=config) as dc:
            for l1_product_name in products:
                files2process, uuids2archive, duplicates = find_to_process(
                    dc,
                    l1_product_name,
                    ancil_config=ancil_config,
                    region_codes_per_sat_key=load_aoi(allowed_codes),
                    interim_days_wait=interim_days_wait,
                    days_to_exclude=days_to_exclude,
                    find_blocked=find_blocked,
                    min_date=default_min_date,
                    max_date=default_max_date,
                )
                uuids2archive_combined += uuids2archive
                paths_to_process.extend(files2process)
                duplicate_count += duplicates

        # If we stopped above as soon as we reached the limit we could end up in a situation where
        # only the first product is ever processed.

        # Sort files so most recent acquisitions are processed first.
        # This is to avoid a backlog holding up recent acquisitions
        paths_to_process.sort(key=_get_path_date, reverse=True)

        # TODO: the old code reduced written records by the duplicate count, seemingly for multi-granule to be counted
        #       multiple times. But it also had a comment saying it wouldn't work well with multi-granule...
        l1_count = min(len(paths_to_process), scene_limit)
        with open(usgs_level1_files, "w") as fid:
            for path in paths_to_process[:l1_count]:
                fid.write(str(path) + "\n")

        generate_ard_job(
            ard_click_params,
            l1_count,
            usgs_level1_files,
            uuids2archive_combined,
            jobdir,
            run_ard,
        )

    _LOG.info("info", jobdir=str(jobdir))


if __name__ == "__main__":
    scene_select()
