#!/usr/bin/env python3

import datetime
import math
import os
import re
import stat
import subprocess
import uuid
from logging.config import fileConfig
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from urllib.parse import urlparse
from urllib.request import url2pathname

import click
import json

try:
    import datacube
except (ImportError, AttributeError):
    print("Could not import Datacube")

from scene_select.check_ancillary import BRDF_DIR, WV_DIR, AncillaryFiles
from scene_select.dass_logs import LOGGER, LogMainFunction

AOI_FILE = "Australian_AOI.json"

DATA_DIR = Path(__file__).parent.joinpath("data")
ODC_FILTERED_FILE = "scenes_to_ARD_process.txt"
ARCHIVE_FILE = "uuid_to_archive.txt"
PRODUCTS = '["usgs_ls7e_level1_2", "usgs_ls8c_level1_2"]'
FMT2 = "filter-jobid-{jobid}"

# Logging
LOG_CONFIG_FILE = "log_config.ini"
GEN_LOG_FILE = "ard_scene_select.log"

# LOGGER events
SCENEREMOVED = "scene removed"
SCENEADDED = "scene added"
SUMMARY = "summary"
MANYSCENES = "Multiple identical ARD scene ids"

# LOGGER keys
DATASETPATH = "dataset_path"
DATASETTIMEEND = "dataset_time_end"
REASON = "reason"
MSG = "message"
DATASETID = "dataset_id"
SCENEID = "landsat_scene_id"
PRODUCTID = "landsat_product_id"


# No such product - "ga_ls8c_level1_3": "ga_ls8c_ard_3",
ARD_PARENT_PRODUCT_MAPPING = {
    "ga_ls5t_level1_3": "ga_ls5t_ard_3",
    "ga_ls7e_level1_3": "ga_ls7e_ard_3",
    "usgs_ls5t_level1_1": "ga_ls5t_ard_3",
    "usgs_ls7e_level1_1": "ga_ls7e_ard_3",
    "usgs_ls7e_level1_2": "ga_ls7e_ard_3",
    "usgs_ls8c_level1_1": "ga_ls8c_ard_3",
    "usgs_ls8c_level1_2": "ga_ls8c_ard_3",
    "esa_s2am_level1_1": "s2a_ard_granule",
    "esa_s2bm_level1_1": "s2b_ard_granule",
}

PBS_JOB = """#!/bin/bash
module purge
module load pbs

source {env}

ard_pbs --level1-list {scene_list} {ard_args}
"""

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

S2_PATTERN = r"^(?P<satellite>S2)" + r"(?P<satelliteid>[A-B])_"

PROCESSING_PATTERN_MAPPING = {
    "ga_ls5t_level1_3": L5_PATTERN,
    "ga_ls7e_level1_3": L7_C1_PATTERN,
    "usgs_ls5t_level1_1": L5_PATTERN,
    "usgs_ls7e_level1_1": L7_C1_PATTERN,
    "usgs_ls7e_level1_2": L7_C2_PATTERN,
    "usgs_ls8c_level1_1": L8_C1_PATTERN,
    "usgs_ls8c_level1_2": L8_C2_PATTERN,
    "esa_s2am_level1_1": S2_PATTERN,
    "esa_s2bm_level1_1": S2_PATTERN,
}


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


def load_aoi(file_name: Path) -> Dict:
    """load a file of region codes."""

    with open(file_name, "r") as f:
        data = json.load(f)

    # json does not save sets
    # So after loading the list is converted to a set
    for key, value in data.items():
        data[key] = set(value)
    return data


def dataset_with_final_child(dc, dataset):
    """
    If any child exists that isn't archived, with a dataset_maturity of 'final'
    :param dc:
    :param dataset:
    :return:
    """
    ds_w_child = []
    for child_dataset in dc.index.datasets.get_derived(dataset.id):
        if (
            not child_dataset.is_archived
            and child_dataset.metadata.dataset_maturity == "final"
        ):
            ds_w_child.append(child_dataset)
    return any(ds_w_child)


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


def calc_processed_ard_scene_ids(dc, product, sat_key):
    """
    Return None or
    a dictionary with key chopped_scene_id and value id, maturity level.
    """
    if product in ARD_PARENT_PRODUCT_MAPPING and sat_key == "ls":
        processed_ard_scene_ids = {}
        for result in dc.index.datasets.search_returning(
            ("landsat_scene_id", "dataset_maturity", "id"),
            product=ARD_PARENT_PRODUCT_MAPPING[product],
        ):
            choppped_id = chopped_scene_id(result.landsat_scene_id)
            if choppped_id in processed_ard_scene_ids:
                # The same chopped scene id has multiple scenes
                old_uuid = processed_ard_scene_ids[choppped_id]["id"]
                LOGGER.warning(
                    MANYSCENES,
                    SCENEID=result.landsat_scene_id,
                    old_uuid=old_uuid,
                    new_uuid=result.id,
                )
            chopped_scene = chopped_scene_id(result.landsat_scene_id)
            processed_ard_scene_ids[chopped_scene] = {
                "dataset_maturity": result.dataset_maturity,
                "id": result.id,
            }
    else:
        # scene select has its own mapping for l1 product to ard product
        # (ARD_PARENT_PRODUCT_MAPPING).
        # If there is a l1 product that is not in this mapping this warning
        # is logged.
        # This uses the l1 product to ard mapping to filter out
        # updated l1 scenes that have been processed using the old l1 scene.
        if product not in ARD_PARENT_PRODUCT_MAPPING:
            LOGGER.warning(
                "THE ARD ODC product name after ARD processing is not known.",
                product=product,
            )
        processed_ard_scene_ids = None
    return processed_ard_scene_ids


def exclude_days(days_to_exclude: List, checkdatetime):
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
    assert len(l1_dataset.uris) == 1
    components = urlparse(l1_dataset.uris[0])
    if not (components.scheme == "file" or components.scheme == "zip"):
        raise ValueError(
            "Only file/Zip URIs currently supported. Tried %r." % components.scheme
        )
    path = url2pathname(components.path)
    if path[-2:] == "!/":
        path = path[:-2]
    return path


def get_aoi_sat_key(region_codes: Dict, product: str):
    aoi_sat_key = None
    for key in region_codes.keys():
        if key in product:
            aoi_sat_key = key
            continue
    return aoi_sat_key


def filter_ancillary(l1_dataset, ancill_there, msg, interim_days_wait, temp_logger):
    # filter out due to ancillary
    # not being there
    filter_out = False
    if ancill_there is False:
        days_ago = datetime.datetime.now(
            l1_dataset.time.end.tzinfo
        ) - datetime.timedelta(days=interim_days_wait)
        if days_ago > l1_dataset.time.end:
            # If the ancillary files take too long to turn up
            # process anyway
            kwargs = {
                "days_ago": str(days_ago),
                "dataset.time.end": str(l1_dataset.time.end),
            }
            temp_logger.debug("No ancillary. Processing to interim", **kwargs)
        else:
            kwargs = {
                REASON: "ancillary files not ready",
                "days_ago": str(days_ago),
                "dataset.time.end": str(l1_dataset.time.end),
                MSG: (f"Not ready: {msg}"),
            }
            temp_logger.info(SCENEREMOVED, **kwargs)
            filter_out = True
    return filter_out


def filter_reprocessed_scenes(
    dc,
    l1_dataset,
    processed_ard_scene_ids,
    find_blocked,
    ancill_there,
    uuids2archive,
    temp_logger,
):

    filter_out = False
    # Do the data with child filter here
    # It will slow things down
    # But any chopped_scene_id in processed_ard_scene_ids
    # will now be a blocked reprocessed scene
    if find_blocked:
        if dataset_with_final_child(dc, l1_dataset):
            temp_logger.debug(
                SCENEREMOVED, **{REASON: "Skipping dataset with children"}
            )
            filter_out = True

    if processed_ard_scene_ids and not filter_out:
        a_scene_id = chopped_scene_id(l1_dataset.metadata.landsat_scene_id)
        if a_scene_id in processed_ard_scene_ids:
            kwargs = {}
            if find_blocked:
                kwargs[REASON] = "Potential blocked reprocessed scene."
                # Since all dataset with final childs
                # have been filtered out
            else:
                kwargs[REASON] = "The scene has been processed"
                # Since dataset with final childs have not been
                # filtered out we don't know why there is
                # an ard there.

            produced_ard = processed_ard_scene_ids[a_scene_id]
            if produced_ard["dataset_maturity"] == "interim" and ancill_there is True:
                # lets build a list of ARD uuid's to delete
                uuids2archive.append(str(produced_ard["id"]))

                temp_logger.debug(
                    SCENEADDED, **{REASON: "Interim scene is being processed to final"}
                )
            else:
                temp_logger.debug(SCENEREMOVED, **kwargs)
                # Contine for everything except interim
                # so it doesn't get processed
                filter_out = True
    return filter_out


def l1_filter(
    dc,
    l1_product,
    brdfdir: Path,
    wvdir: Path,
    region_codes: Dict,
    interim_days_wait: int,
    days_to_exclude: List,
    find_blocked: bool,
):

    """return
    @param dc:
    @param l1_product: l1 product
    @param brdfdir:
    @param wvdir:
    @param region_codes:
    @param interim_days_wait:
    @param days_to_exclude:
    @param find_blocked:
    @return: a list of file paths to ARD process
    """
    # pylint: disable=R0914
    # R0914: Too many local variables

    sat_key = get_aoi_sat_key(region_codes, l1_product)

    # This is used to block reprocessing of reprocessed l1's
    processed_ard_scene_ids = calc_processed_ard_scene_ids(dc, l1_product, sat_key)

    # LOGGER.debug("location:pre-AncillaryFiles")
    ancillary_ob = AncillaryFiles(brdf_dir=brdfdir, wv_dir=wvdir)
    # LOGGER.debug("location:post-AncillaryFiles")
    files2process = []
    uuids2archive = []
    for l1_dataset in dc.index.datasets.search(product=l1_product):
        if sat_key == "ls":
            product_id = l1_dataset.metadata.landsat_product_id
            sceneid = l1_dataset.metadata.landsat_scene_id
        elif sat_key == "s2":
            product_id = l1_dataset.metadata.sentinel_tile_id
            sceneid = None
        region_code = l1_dataset.metadata.region_code
        file_path = calc_file_path(l1_dataset, product_id)
        # Set up the logging
        temp_logger = LOGGER.bind(
            SCENEID=product_id, DATASETID=str(l1_dataset.id), DATASETPATH=file_path
        )

        # Filter out if the processing level is too low
        prod_pattern = PROCESSING_PATTERN_MAPPING[l1_product]
        if not re.match(prod_pattern, product_id):
            temp_logger.debug(SCENEREMOVED, **{REASON: "Processing level too low"})
            continue

        # Filter out if outside area of interest
        if sat_key is not None and region_code not in region_codes[sat_key]:
            kwargs = {
                REASON: "Region not in AOI",
                "region_code": region_code,
            }
            temp_logger.debug(SCENEREMOVED, **kwargs)
            continue

        ancill_there, msg = ancillary_ob.ancillary_files(l1_dataset.time.end)
        # Continue here if a maturity level of final cannot be produced
        # since the ancillary files are not there
        if filter_ancillary(
            l1_dataset, ancill_there, msg, interim_days_wait, temp_logger
        ):
            continue

        # FIXME remove the hard-coded list
        if exclude_days(days_to_exclude, l1_dataset.time.end):
            kwargs = {
                DATASETTIMEEND: l1_dataset.time.end,
                REASON: "This day is excluded.",
            }
            temp_logger.info(SCENEREMOVED, **kwargs)
            continue

        if filter_reprocessed_scenes(
            dc,
            l1_dataset,
            processed_ard_scene_ids,
            find_blocked,
            ancill_there,
            uuids2archive,
            temp_logger,
        ):
            continue

        # WARNING any filter under here will
        # be executed on interim scenes that it is assumed will
        # be processed

        # LOGGER.debug("location:pre dataset_with_final_child")
        # If any child exists that isn't archived
        if dataset_with_final_child(dc, l1_dataset):
            temp_logger.debug(
                SCENEREMOVED, **{REASON: "Skipping dataset with children"}
            )
            continue

        files2process.append(file_path)

    return files2process, uuids2archive


def l1_scenes_to_process(
    outfile: Path,
    products: List[str],
    brdfdir: Path,
    wvdir: Path,
    region_codes: Dict,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: List,
    find_blocked: bool,
    config: Optional[Path] = None,
) -> Tuple[int, List[str]]:
    """Writes all the files returned from datacube for level1 to a file."""
    # pylint: disable=R0914
    # R0914: Too many local variables

    dc = datacube.Datacube(app="gen-list", config=config)
    l1_count = 0
    with open(outfile, "w") as fid:
        uuids2archive_combined = []
        for product in products:
            files2process, uuids2archive = l1_filter(
                dc,
                product,
                brdfdir=brdfdir,
                wvdir=wvdir,
                region_codes=region_codes,
                interim_days_wait=interim_days_wait,
                days_to_exclude=days_to_exclude,
                find_blocked=find_blocked,
            )
            uuids2archive_combined += uuids2archive
            for fp in files2process:
                fid.write(str(fp) + "\n")
                l1_count += 1
                if l1_count >= scene_limit:
                    break
            if l1_count >= scene_limit:
                break
    return l1_count, uuids2archive_combined


def _calc_node_with_defaults(ard_click_params, count_all_scenes_list):
    # Estimate the number of nodes needed

    hours_per_granule = 7.5
    if ard_click_params["nodes"] is None:
        if ard_click_params["walltime"] is None:
            walltime = "10:00:00"
        else:
            walltime = ard_click_params["walltime"]
        if ard_click_params["workers"] is None:
            workers = 30
        else:
            workers = ard_click_params["workers"]
        ard_click_params["nodes"] = _calc_nodes_req(
            count_all_scenes_list, walltime, workers, hours_per_granule
        )
    hours, _, _ = (int(x) for x in walltime.split(":"))

    if hours <= hours_per_granule:
        raise ValueError("wall time <= hours per granule")


def _calc_nodes_req(granule_count, walltime, workers, hours_per_granule=7.5):
    """Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59', 28)
    2
    >>> _calc_nodes_req(800, '20:00', 28)
    3
    """
    hours, _, _ = (int(x) for x in walltime.split(":"))
    # to avoid divide by zero errors
    if hours == 0:
        hours = 1
    total_hours = float(hours_per_granule * granule_count)
    nodes = int(math.ceil(total_hours / (hours * workers)))
    if nodes == 0:
        # A zero node request to ard causes errors.
        nodes = 1
    return nodes


def dict2ard_arg_string(ard_click_params):
    ard_params = []
    for key, value in ard_click_params.items():
        if value is None:
            continue
        if key == "test":
            if value is True:
                ard_params.append("--" + key)
            continue
        if key == "yamls_dir":
            if value == "":
                # remove the yamls-dir if it is empty
                continue
        # convert underscores to dashes
        key = key.replace("_", "-")
        ard_params.append("--" + key)
        # Make path strings absolute
        if key in ("workdir", "logdir", "pkgdir", "index-datacube-env"):
            value = Path(value).resolve()
        ard_params.append(str(value))
    ard_arg_string = " ".join(ard_params)
    return ard_arg_string


def make_ard_pbs(level1_list, **ard_click_params):

    if ard_click_params["env"] is None:
        # Don't error out, just
        # fill env with a bad value
        env = "None"
    else:
        env = Path(ard_click_params["env"]).resolve()

    ard_args = dict2ard_arg_string(ard_click_params)
    pbs = PBS_JOB.format(env=env, scene_list=level1_list, ard_args=ard_args)
    return pbs


@click.command()
@click.option(
    "--usgs-level1-files",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a text files containing all "
    "the level-1 USGS/ESA list to be filtered",
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(AOI_FILE),
    help="full path to a json file containing path/row and "
    "MGRS tiles to act as a area of interest filter",
)
@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="Full path to a datacube config text file."
    " This describes the ODC database.",
    default=None,
)
@click.option(
    "--products",
    cls=PythonLiteralOption,
    type=list,
    help="List the ODC products to be processed. e.g."
    ' \'["ga_ls5t_level1_3", "usgs_ls8c_level1_1"]\'',
    default=PRODUCTS,
)
@click.option(
    "--workdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output working directory.",
    default=Path.cwd(),
)
@click.option(
    "--brdfdir",
    type=click.Path(file_okay=False),
    help="The home directory of BRDF data used by scene select.",
    default=BRDF_DIR,
)
@click.option(
    "--wvdir",
    type=click.Path(file_okay=False),
    help="The home directory of water vapour data used by scene select.",
    default=WV_DIR,
)
@click.option(
    "--scene-limit",
    default=300,
    type=int,
    help="Safety limit: Maximum number of scenes to process in a run.",
)
@click.option(
    "--interim-days-wait",
    default=35,
    type=int,
    help="Maxi days to wait for ancillary data before processing ARD to "
    "an interim maturity level.",
)
@click.option(
    "--days-to-exclude",
    cls=PythonLiteralOption,
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
    help="Produce ARD scenes by executing the ard_pbs script.",
)
# These are passed on to ard processing
@click.option(
    "--test",
    default=False,
    is_flag=True,
    help="Test job execution (Don't submit the job to the PBS queue).",
)
@click.option(
    "--log-config",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(LOG_CONFIG_FILE),
    help="full path to the logging configuration file",
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
@LogMainFunction()
def scene_select(
    usgs_level1_files: click.Path,
    allowed_codes: click.Path,
    config: click.Path,
    products: list,
    logdir: click.Path,
    brdfdir: click.Path,
    wvdir: click.Path,
    stop_logging: bool,
    log_config: click.Path,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: list,
    run_ard: bool,
    find_blocked: bool,
    **ard_click_params: dict,
):
    """
    The keys for ard_click_params;
        test: bool,
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
    jobdir = logdir.joinpath(FMT2.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    # FIXME test this
    if not stop_logging:
        gen_log_file = jobdir.joinpath(GEN_LOG_FILE).resolve()
        fileConfig(
            log_config,
            disable_existing_loggers=False,
            defaults={"genlogfilename": str(gen_log_file)},
        )
    LOGGER.info("scene_select", **locals())

    # logdir is used both  by scene select and ard
    # So put it in the ard parameter dictionary
    ard_click_params["logdir"] = logdir

    if not usgs_level1_files:
        usgs_level1_files = jobdir.joinpath(ODC_FILTERED_FILE)
        l1_count, uuids2archive = l1_scenes_to_process(
            usgs_level1_files,
            products=products,
            brdfdir=Path(brdfdir).resolve(),
            wvdir=Path(wvdir).resolve(),
            region_codes=load_aoi(allowed_codes),
            config=config,
            scene_limit=scene_limit,
            interim_days_wait=interim_days_wait,
            days_to_exclude=days_to_exclude,
            find_blocked=find_blocked,
        )
        # ARCHIVE_FILE
        path_scenes_to_archive = jobdir.joinpath(ARCHIVE_FILE)
        with open(path_scenes_to_archive, "w") as fid:
            fid.write("\n".join(uuids2archive))
    else:
        uuids2archive = []
        l1_count = sum(1 for _ in open(usgs_level1_files))

    try:
        _calc_node_with_defaults(ard_click_params, l1_count)
    except ValueError as err:
        print(err.args)
        LOGGER.warning("ValueError", message=err.args)

    # write pbs script
    if len(uuids2archive) > 0:
        ard_click_params["archive-list"] = path_scenes_to_archive
    script_path = jobdir.joinpath("run_ard_pbs.sh")
    with open(script_path, "w") as src:
        src.write(make_ard_pbs(usgs_level1_files, **ard_click_params))

    # Make the script executable
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

    # run the script
    if run_ard is True:
        subprocess.run([script_path], check=True)

    LOGGER.info("info", jobdir=str(jobdir))
    print("Job directory: " + str(jobdir))


if __name__ == "__main__":
    scene_select()
