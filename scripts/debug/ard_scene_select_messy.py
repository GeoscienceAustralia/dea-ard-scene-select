#!/usr/bin/env python3

import os
import stat
import math
from pathlib import Path
from typing import List, Tuple, Optional
import re
import uuid
import subprocess
import datetime
import click
from logging.config import fileConfig

import pytz

import pprint
import sys
import time
import timeit

#dc = None

try:
    import datacube
except (ImportError, AttributeError) as error:
    print("Could not import Datacube")

from scene_select.check_ancillary import AncillaryFiles, BRDF_DIR, WV_DIR
from scene_select.dass_logs import LOGGER, LogMainFunction

LANDSAT_AOI_FILE = "Australian_Wrs_list.txt"
DATA_DIR = Path(__file__).parent.joinpath("data")
ODC_FILTERED_FILE = "scenes_to_ARD_process.txt"
ARCHIVE_FILE = "uuid_to_archive.txt"
PRODUCTS = '["ga_ls5t_level1_3", "ga_ls7e_level1_3", \
                    "usgs_ls5t_level1_1", "usgs_ls7e_level1_1", "usgs_ls8c_level1_1"]'
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

# No such product - "ga_ls8c_level1_3": "ga_ls8c_ard_3",
ARD_PARENT_PRODUCT_MAPPING = {
    "ga_ls5t_level1_3": "ga_ls5t_ard_3",
    "ga_ls7e_level1_3": "ga_ls7e_ard_3",
    "usgs_ls5t_level1_1": "ga_ls5t_ard_3",
    "usgs_ls7e_level1_1": "ga_ls7e_ard_3",
    "usgs_ls8c_level1_1": "ga_ls8c_ard_3",
}

NODE_TEMPLATE = """#!/bin/bash
module purge
module load pbs

source {env}

ard_pbs --level1-list {scene_list} {ard_args}
"""

# landsat 8 filename pattern is configured to match only
# processing level L1TP and L1GT for acquisition containing
# both the TIRS and OLI sensors with .tar extension.
L8_PATTERN = (
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

# L1TP and L1GT are all ortho-rectified with DEM.
# The only difference is L1GT was processed without Ground Control Points
# - but because LS8 orbit is very accurate so LS8 L1GT products with orbital
# info is ~90% within one pixel.
# (From Lan-Wei)
# Therefore we use L1GT for ls8 but not ls7 or ls5.

# landsat 7 filename pattern is configured to match only
# processing level L1TP with .tar extension.
L7_PATTERN = (
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

PROCESSING_PATTERN_MAPPING = {
    "ga_ls5t_level1_3": L5_PATTERN,
    "ga_ls7e_level1_3": L7_PATTERN,
    "usgs_ls5t_level1_1": L5_PATTERN,
    "usgs_ls7e_level1_1": L7_PATTERN,
    "usgs_ls8c_level1_1": L8_PATTERN,
}


class PythonLiteralOption(click.Option):
    """Load click value representing a Python list. """

    def type_cast_value(self, ctx, value):
        try:
            value = str(value)
            assert value.count("[") == 1 and value.count("]") == 1
            list_as_str = value.replace('"', "'").split("[")[1].split("]")[0]
            list_of_items = [item.strip().strip("'") for item in list_as_str.split(",")]
            if list_of_items == [""]:
                list_of_items = []
            return list_of_items
        except Exception:
            raise click.BadParameter(value)


def allowed_codes_to_region_codes(allowed_codes: Path) -> List:
    """ Convert a file of allowed codes to a list of region codes. """
    with open(allowed_codes, "r") as fid:
        path_row_list = [line.rstrip() for line in fid.readlines()]
    path_row_list = ["{:03}{:03}".format(int(item.split("_")[0]), int(item.split("_")[1])) for item in path_row_list]
    return path_row_list


def dataset_with_child(dc, dataset):
    """
    If any child exists that isn't archived
    :param dc:
    :param dataset:
    :return:
    """
    return any(not child_dataset.is_archived for child_dataset in dc.index.datasets.get_derived(dataset.id))


def dataset_with_child_dev(dc, dataset):
    """
    If any child exists that isn't archived
    :param dc:
    :param dataset:
    :return:
    """
    ds_w_child = []
    for child_dataset in dc.index.datasets.get_derived(dataset.id):
        if not child_dataset.is_archived:  # and not dataset.dataset_maturity:
            ds_w_child.append(child_dataset)
    print("***** ds_w_child *******")
    print(ds_w_child)
    return any(not child_dataset.is_archived for child_dataset in dc.index.datasets.get_derived(dataset.id))


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

def search_returning_landsat_product_id():
    for result in dc_global.index.datasets.search_returning(
            ("landsat_product_id",),
            product='usgs_ls8c_level1_1'
        ):
        print (result.landsat_product_id)

def search_landsat_product_id():
    for result in dc_global.index.datasets.search(
            product='usgs_ls8c_level1_1'
        ):
        #print (result.metadata_doc)
        #print (result.metadata_doc["properties"])
        print (result.metadata_doc["properties"]["landsat:landsat_product_id"])
        #break



def search_summaries_landsat_product_id():
    for result in dc_global.index.datasets.search_summaries(
            product='usgs_ls8c_level1_1'
        ):
        #print (result.metadata_doc)
        #print (result.metadata_doc["properties"])
        print (result["metadata_doc"]["properties"]["landsat:landsat_product_id"])
        

def calc_processed_ard_scene_ids(dc, product):
    """Return None or a dictionary with key chopped_scene_id and value  maturity level.
"""
    global dc_global
    dc_global = dc

    if False and product in ARD_PARENT_PRODUCT_MAPPING:
        for prod in [product, ARD_PARENT_PRODUCT_MAPPING[product]]:
            #prod = product
            print("**********     get_field_names   {} ************".format(prod))
            field_names = dc.index.datasets.get_field_names(product_name=prod)
            print(field_names)
            for a_name in ['local_path', 'landsat_product_id', 'region_code',
                       'id', 'time', ]:
                if a_name in field_names:
                    print ("{} is in".format(a_name))
                else:
                    print ("{} OUT!!!".format(a_name))
    if False:
        t0 = time.time()
        for result in dc.index.datasets.search_returning(
                ("landsat_product_id",),
                product='usgs_ls8c_level1_1'
        ):
            print (result["landsat_product_id"])
        t1 = time.time()
        
        total = t1-t0
        print ("total")
        print (total)
    print ('timeit start')
    atime_search_returning_landsat_product_id = None
    atime_search_summaries_landsat_product_id = None
    atime_search_landsat_product_id = None
    #print (timeit.timeit(search_returning_landsat_product_id, number=20))
    atime_search_returning_landsat_product_id = min(timeit.Timer(search_returning_landsat_product_id).repeat(repeat=10, number=1))
    atime_search_summaries_landsat_product_id = min(timeit.Timer(search_summaries_landsat_product_id).repeat(repeat=10, number=1))
    atime_search_landsat_product_id = min(timeit.Timer(search_landsat_product_id).repeat(repeat=10, number=1))
    print ('atime_search_returning_landsat_product_id')
    print (atime_search_returning_landsat_product_id)
    print ('atime_search_landsat_product_id')
    print (atime_search_landsat_product_id)
    print ('atime_search_summaries_landsat_product_id')
    print (atime_search_summaries_landsat_product_id)
    print ('timeit end')
    sys.exit(0)

    """
atime_search_returning_landsat_product_id
3.427654878993053
atime_search_landsat_product_id
18.267368010012433

    t0 = time.time()
    code_block
    t1 = time.time()
    
    total = t1-t0
    """
    datasets = list(dc.index.datasets.search(product=product))
    print (datasets[0].local_path)
    print (datasets[0].metadata_doc)
    #print (datasets[0].metadata_doc['uri'])

    sys.exit(0)
    #    file_path = (
    #        dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix(

    if product in ARD_PARENT_PRODUCT_MAPPING:
        print("**********    SUMMARIES    ************")
        summy = dc.index.datasets.search_summaries(product=ARD_PARENT_PRODUCT_MAPPING[product])

        # pprint.pprint (list(summy)) # good
        for result in summy:
            # pprint.pprint (result)
            pprint.pprint(result["id"])
            pprint.pprint(result["landsat_scene_id"])  # landsat_scene_id
            pprint.pprint(result["dataset_maturity"])
            pprint.pprint(result["metadata_doc"]["properties"]["landsat:landsat_product_id"])
        sys.exit(0)
        for result in dc.index.datasets.search_eager(product=ARD_PARENT_PRODUCT_MAPPING[product]):
            print("**********list(result) ")
            print(result)
            # print (list(result)) # not iterable
            print("********* metadata_doc  ")
            print(result.metadata_doc)
            # print (result['metadata_doc']) # TypeError: 'Dataset' object is not subscriptable
            # print (list(result.metadata_doc))
            print("********* metadata_doc ['lineage']['source_datasets'] ")
            # print (list(result.metadata_doc['accessories'])) # no attribute
            print(list(result.metadata_doc["lineage"]["source_datasets"]))
            print("**********landsat:landsat_product_id   ) ")
            print(result.metadata_doc["properties"]["landsat:landsat_product_id"])
            # print (list(result.metadata_doc.lineage.source_datasets))
            print(result.metadata)  # <datacube.utils.documents.DocReader object at 0x7f61d29eaa90>
            # print (result.metadata.fields) # the mother load. Excellent data
            # But no lineage
            # print (result.fields) # nope
            # print (result.source_datasets) # nope
            # print (result.lineage) nope
            # print (result.lineage) nope
        # for result in dc.index.datasets.search_returning(("landsat_scene_id", "dataset_maturity", "id"), product=ARD_PARENT_PRODUCT_MAPPING[product]

        processed_ard_scene_ids = {}
        for result in dc.index.datasets.search_returning(
            ("landsat_scene_id", "dataset_maturity", "id"), product=ARD_PARENT_PRODUCT_MAPPING[product]
        ):
            # bad fields
            #  "properties"
            # "landsat"
            # print ("result.stuff")
            # print (result.metadata)
            # ("landsat_scene_id", "dataset_maturity", "id"),
            choppped_id = chopped_scene_id(result.landsat_scene_id)
            if choppped_id in processed_ard_scene_ids:
                # The same chopped scene id has multiple scenes
                old_uuid = processed_ard_scene_ids[choppped_id]["id"]
                LOGGER.warning(MANYSCENES, SCENEID=result.landsat_scene_id, old_uuid=old_uuid, new_uuid=result.id)

            processed_ard_scene_ids[chopped_scene_id(result.landsat_scene_id)] = {
                "dataset_maturity": result.dataset_maturity,
                "id": result.id,
            }
        for result in dc.index.datasets.search_summaries(product=ARD_PARENT_PRODUCT_MAPPING[product]):
            print("pprint (list(result.metadata_doc))")
            pprint.pprint(list(result.metadata_doc))
            pprint.pprint(result.metadata_doc)

            # bad fields
            #  "properties"
            # "landsat"
            # print ("result.stuff")
            # print (result.metadata)
            # ("landsat_scene_id", "dataset_maturity", "id"),
            #
            print(list(result.metadata_doc["lineage"]["source_datasets"]))
            print("**********landsat:landsat_product_id   ) ")
            print(result.metadata_doc["properties"]["landsat:landsat_product_id"])
            # pprint (list(result))
            choppped_id = chopped_scene_id(result.metadata_doc.landsat_scene_id)
            if choppped_id in processed_ard_scene_ids:
                # The same chopped scene id has multiple scenes
                old_uuid = processed_ard_scene_ids[choppped_id]["id"]
                LOGGER.warning(MANYSCENES, SCENEID=result.landsat_scene_id, old_uuid=old_uuid, new_uuid=result.id)

            processed_ard_scene_ids[chopped_scene_id(result.landsat_scene_id)] = {
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
        processed_ard_scene_ids = None
        LOGGER.warning("THE ARD ODC product name after ARD processing is not known.", product=product)
    return processed_ard_scene_ids


def exclude_days(days_to_exclude: List, checkdatetime):
    """
    days_to_exclude format example; '["2020-08-09:2020-08-30", "2020-09-02:2020-09-05"]'
    """
    for period in days_to_exclude:
        start, end = period.split(":")
        # datetime.timezone.utc
        # pytz.UTC
        start = datetime.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=checkdatetime.tzinfo)
        end = datetime.datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=checkdatetime.tzinfo)

        # let's make it the end of the day
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        if start <= checkdatetime <= end:
            return True
    return False


def l1_filter(
    dc, product, brdfdir: Path, wvdir: Path, region_codes: List, interim_days_wait: int, days_to_exclude: List,
):
    """return a list of file paths to ARD process """
    # pylint: disable=R0914
    # R0914: Too many local variables

    processed_ard_scene_ids = calc_processed_ard_scene_ids(dc, product)
    ancillary_ob = AncillaryFiles(brdf_dir=brdfdir, water_vapour_dir=wvdir)
    files2process = []
    uuids2archive = []
    for dataset in dc.index.datasets.search(product=product):
        file_path = (
            dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix()
        )

        # Filter out if the processing level is too low
        if not re.match(PROCESSING_PATTERN_MAPPING[product], dataset.metadata.landsat_product_id):

            kwargs = {REASON: "Processing level too low, new ", SCENEID: dataset.metadata.landsat_scene_id}
            LOGGER.debug(SCENEREMOVED, **kwargs)
            continue

        # Filter out if outside area of interest
        if dataset.metadata.region_code not in region_codes:
            kwargs = {
                SCENEID: dataset.metadata.landsat_scene_id,
                REASON: "Region not in AOI",
                MSG: ("Path row %s" % dataset.metadata.region_code),
            }
            LOGGER.debug(SCENEREMOVED, **kwargs)
            continue

        if not dataset.local_path:
            kwargs = {
                DATASETID: str(dataset.id),
                REASON: "Skipping dataset without local paths",
                MSG: "Bad scene format",
            }
            LOGGER.warning(SCENEREMOVED, **kwargs)
            continue

        assert dataset.local_path.name.endswith("metadata.yaml")

        # Continue here if a maturity level of final cannot be procduced
        # since the ancillary files are not there
        ancill_there, msg = ancillary_ob.definitive_ancillary_files(dataset.time.end)
        if ancill_there is False:
            days_ago = datetime.datetime.now(dataset.time.end.tzinfo) - datetime.timedelta(days=interim_days_wait)
            kwargs = {
                "days_ago": str(days_ago),
                "dataset.time.end": str(dataset.time.end),
                SCENEID: dataset.metadata.landsat_scene_id,
                MSG: ("ancillary info %s" % msg),
            }
            LOGGER.debug("no ancillary", **kwargs)
            if days_ago > dataset.time.end:
                # If the ancillary files take too long to turn up
                # process anyway
                kwargs = {
                    DATASETPATH: file_path,
                    SCENEID: dataset.metadata.landsat_scene_id,
                }
                LOGGER.debug("Fallback to interim", **kwargs)
            else:
                kwargs = {
                    DATASETPATH: file_path,
                    SCENEID: dataset.metadata.landsat_scene_id,
                    REASON: "ancillary files not ready",
                    MSG: ("Not ready: %s" % msg),
                }
                LOGGER.info(SCENEREMOVED, **kwargs)
                continue

        # FIXME remove the hard-coded list
        if exclude_days(days_to_exclude, dataset.time.end):
            kwargs = {
                DATASETTIMEEND: dataset.time.end,
                SCENEID: dataset.metadata.landsat_scene_id,
                REASON: "This day is excluded.",
            }
            LOGGER.info(SCENEREMOVED, **kwargs)
            continue

        if processed_ard_scene_ids:
            a_chopped_scene_id = chopped_scene_id(dataset.metadata.landsat_scene_id)

            if a_chopped_scene_id in processed_ard_scene_ids:
                ard_element = processed_ard_scene_ids[a_chopped_scene_id]
                print("*********** ard_element")
                print(ard_element)
                if False:  # dataset.metadata.landsat_product_id == ard_element[landsat_product_id]:
                    kwargs = {
                        DATASETPATH: file_path,
                        REASON: "Re-processed scene.  Manually remove old scenes.",
                        SCENEID: dataset.metadata.landsat_scene_id,
                    }

                kwargs = {
                    DATASETPATH: file_path,
                    REASON: "The scene has been ARD processed",
                    SCENEID: dataset.metadata.landsat_scene_id,
                }
                LOGGER.debug(SCENEREMOVED, **kwargs)
                produced_ard = processed_ard_scene_ids[a_chopped_scene_id]
            # FixME remove
            if dataset_with_child(dc, dataset):
                kwargs = {
                    DATASETPATH: file_path,
                    REASON: "Skipping dataset with children deep base remix",
                    SCENEID: dataset.metadata.landsat_scene_id,
                }
                LOGGER.debug(SCENEREMOVED, **kwargs)
                if produced_ard["dataset_maturity"] == "interim" and ancill_there is True:
                    # lets build a list of ARD uuid's to delete
                    uuids2archive.append(str(produced_ard["id"]))

                    # Let's reprocess this file to final
                    # skipping the 'any child exists that isn't archived'
                    # filter
                    files2process.append(file_path)
                continue

        # WARNING any filter under here will not be executed when processing interim scenes

        # If any child exists that isn't archived
        if dataset_with_child(dc, dataset):
            kwargs = {
                DATASETPATH: file_path,
                REASON: "Skipping dataset with children",
                SCENEID: dataset.metadata.landsat_scene_id,
            }
            LOGGER.debug(SCENEREMOVED, **kwargs)
            continue

        files2process.append(file_path)

    return files2process, uuids2archive


def l1_scenes_to_process(
    outfile: Path,
    products: List[str],
    brdfdir: Path,
    wvdir: Path,
    region_codes: List,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: List,
    config: Optional[Path] = None,
) -> Tuple[int, List[str]]:
    """Writes all the files returned from datacube for level1 to a text file."""
    dc = datacube.Datacube(app="gen-list", config=config)
    l1_count = 0
    with open(outfile, "w") as fid:
        for product in products:
            files2process, uuids2archive = l1_filter(
                dc,
                product,
                brdfdir=brdfdir,
                wvdir=wvdir,
                region_codes=region_codes,
                interim_days_wait=interim_days_wait,
                days_to_exclude=days_to_exclude,
            )
            for fp in files2process:
                fid.write(fp + "\n")
                l1_count += 1
                if l1_count >= scene_limit:
                    break
            if l1_count >= scene_limit:
                break
    return l1_count, uuids2archive


def _calc_node_with_defaults(ard_click_params, count_all_scenes_list):
    # Estimate the number of nodes needed
    if ard_click_params["nodes"] is None:
        if ard_click_params["walltime"] is None:
            walltime = "05:00:00"
        else:
            walltime = ard_click_params["walltime"]
        if ard_click_params["workers"] is None:
            workers = 30
        else:
            workers = ard_click_params["workers"]
        ard_click_params["nodes"] = _calc_nodes_req(count_all_scenes_list, walltime, workers)


def _calc_nodes_req(granule_count, walltime, workers, hours_per_granule=1.5):
    """ Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59', 28)
    2
    >>> _calc_nodes_req(800, '20:00', 28)
    3
    """
    hours, _, _ = [int(x) for x in walltime.split(":")]
    # to avoid divide by zero errors
    if hours == 0:
        hours = 1
    nodes = int(math.ceil(float(hours_per_granule * granule_count) / (hours * workers)))
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

    ard_args_str = dict2ard_arg_string(ard_click_params)
    pbs = NODE_TEMPLATE.format(env=env, scene_list=level1_list, ard_args=ard_args_str)
    return pbs


@click.command()
@click.option(
    "--usgs-level1-files",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a text files containing all the level-1 USGS/ESA list to be filtered",
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(LANDSAT_AOI_FILE),
    help="full path to a text files containing path/row or MGRS tile name to act as a filter",
)
@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="Full path to a datacube config text file. This describes the ODC database.",
    default=None,
)
@click.option(
    "--products",
    cls=PythonLiteralOption,
    type=list,
    help='List the ODC products to be processed. e.g. \
    \'["ga_ls5t_level1_3", "usgs_ls8c_level1_1"]\'',
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
    help="Maximum number of scenes to process in a run.  This is a safety limit.",
)
@click.option(
    "--interim-days-wait",
    default=35,
    type=int,
    help="Maximum number of days to wait for ancillary data before processing ARD to an interim maturity level.",
)
@click.option(
    "--days-to-exclude",
    cls=PythonLiteralOption,
    type=list,
    help='List of ranges of dates to not process, as (start date: end date) with format (yyyy-mm-dd:yyyy-mm-dd). e.g. \
    \'["2019-12-22:2019-12-25", "2020-08-09:2020-09-03"]\'',
    default=[],
)
@click.option("--run-ard", default=False, is_flag=True, help="Execute the ard_pbs script.")
# These are passed on to ard processing
@click.option("--test", default=False, is_flag=True, help="Test job execution (Don't submit the job to the PBS queue).")
@click.option(
    "--log-config",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(LOG_CONFIG_FILE),
    help="full path to the logging configuration file",
)
@click.option("--stop-logging", default=False, is_flag=True, help="Do not run logging.")
@click.option("--walltime", help="Job walltime in `hh:mm:ss` format.")
@click.option("--email", help="Notification email address.")
@click.option("--project", default="v10", help="Project code to run under.")
@click.option(
    "--logdir", type=click.Path(file_okay=False, writable=True), help="The base logging and scripts output directory."
)
@click.option("--pkgdir", type=click.Path(file_okay=False, writable=True), help="The base output packaged directory.")
@click.option("--env", type=click.Path(exists=True, readable=True), help="Environment script to source.")
@click.option(
    "--index-datacube-env",
    type=click.Path(exists=True, readable=True),
    help="Path to the datacube indexing environment. "
    "Add this to index the ARD results.  "
    "If this option is not defined the ARD results will not be automatically indexed.",
)
@click.option("--workers", type=click.IntRange(1, 48), help="The number of workers to request per node.")
@click.option("--nodes", help="The number of nodes to request.")
@click.option("--memory", help="The memory in GB to request per node.")
@click.option("--jobfs", help="The jobfs memory in GB to request per node.")
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
        fileConfig(log_config, disable_existing_loggers=False, defaults={"genlogfilename": str(gen_log_file)})
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
            region_codes=allowed_codes_to_region_codes(allowed_codes),
            config=config,
            scene_limit=scene_limit,
            interim_days_wait=interim_days_wait,
            days_to_exclude=days_to_exclude,
        )
        # ARCHIVE_FILE
        path_scenes_to_archive = jobdir.joinpath(ARCHIVE_FILE)
        with open(path_scenes_to_archive, "w") as fid:
            for item in uuids2archive:
                fid.write("%s\n" % item)
    else:
        with open(usgs_level1_files) as f:
            i = 0
            for i, l in enumerate(f):
                pass
            l1_count = i + 1

    _calc_node_with_defaults(ard_click_params, l1_count)

    # write pbs script
    if len(uuids2archive) > 0:
        ard_click_params["archive-list"] = path_scenes_to_archive
    run_ard_pathfile = jobdir.joinpath("run_ard_pbs.sh")
    with open(run_ard_pathfile, "w") as src:
        src.write(make_ard_pbs(usgs_level1_files, **ard_click_params))

    # Make the script executable
    os.chmod(run_ard_pathfile, os.stat(run_ard_pathfile).st_mode | stat.S_IEXEC)

    # run the script
    if run_ard is True:
        subprocess.run([run_ard_pathfile], check=True)

    LOGGER.info("info", jobdir=str(jobdir))
    print("Job directory: " + str(jobdir))


if __name__ == "__main__":
    scene_select()