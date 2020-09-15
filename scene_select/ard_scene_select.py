#!/usr/bin/env python3

import os
import sys
import stat
import math
import logging
from pathlib import Path
from typing import List, Optional, Union
import re
import concurrent.futures
import uuid
import subprocess
from datetime import datetime, timedelta
import click
from logging.config import fileConfig

try:
    import datacube
except (ImportError, AttributeError) as error:
    print("Could not import Datacube")

from scene_select.check_ancillary import AncillaryFiles, BRDF_DIR, WV_DIR
from scene_select.dass_logs import LOGGER, LogMainFunction

LANDSAT_AOI_FILE = "Australian_Wrs_list.txt"
LOG_CONFIG_FILE = "log_config.ini"
EXTENT_DIR = Path(__file__).parent.joinpath("auxiliary_extents")
GLOBAL_MGRS_WRS_DIR = Path(__file__).parent.joinpath("global_wrs_mgrs_shps")
DATA_DIR = Path(__file__).parent.joinpath("data")
ODC_FILTERED_FILE = "DataCube_all_landsat_scenes.txt"
LOG_FILE = "ignored_scenes_list.log"
GEN_LOG_FILE = "ard_scene_select.log"
PRODUCTS = '["ga_ls5t_level1_3", "ga_ls7e_level1_3", \
                    "usgs_ls5t_level1_1", "usgs_ls7e_level1_1", "usgs_ls8c_level1_1"]'
FMT2 = "filter-jobid-{jobid}"

BRDFSHAPEFILE = EXTENT_DIR.joinpath("brdf_tiles_new.shp")
ONEDEGDSMV1SHAPEFILE = EXTENT_DIR.joinpath("one-deg-dsm-v1.shp")
ONESECDSMV1SHAPEFILE = EXTENT_DIR.joinpath("one-sec-dsm-v1.shp")
ONEDEGDSMV2SHAPEFILE = EXTENT_DIR.joinpath("one-deg-dsm-v2.shp")
AEROSOLSHAPEFILE = EXTENT_DIR.joinpath("aerosol.shp")
WRSSHAPEFILE = GLOBAL_MGRS_WRS_DIR.joinpath("wrsdall_Decending.shp")
MGRSSHAPEFILE = GLOBAL_MGRS_WRS_DIR.joinpath("S2_tile.shp")

# LOGGER events
SCENEREMOVED = "scene removed"
SCENEADDED = "scene added"
SUMMARY = "summary"

# LOGGER keys
DATASETPATH = "dataset_path"
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
    r"(?P<extension>.tar)$"
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
    r"(?P<extension>.tar)$"
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
    r"(?P<extension>.tar)$"
)


class PythonLiteralOption(click.Option):
    """Load click value representing a Python list. """

    def type_cast_value(self, ctx, value):
        try:
            value = str(value)
            assert value.count("[") == 1 and value.count("]") == 1
            list_as_str = value.replace('"', "'").split("[")[1].split("]")[0]
            list_of_items = [item.strip().strip("'") for item in list_as_str.split(",")]
            return list_of_items
        except Exception:
            raise click.BadParameter(value)


def write(filename: Path, list_to_write: List) -> None:
    """A helper method to write contents in a list to a file."""
    with open(filename, "w") as fid:
        for item in list_to_write:
            fid.write(item + "\n")


def path_row_filter(
    scenes_to_filter_list: Union[List[str], Path],
    path_row_list: Union[List[str], Path],
    out_dir: Path,
    scene_limit=None,
) -> None:
    """Filter scenes to check if path/row of a scene is allowed in a path row list."""

    if isinstance(path_row_list, Path):
        with open(path_row_list, "r") as fid:
            path_row_list = [line.rstrip() for line in fid.readlines()]

    path_row_list = ["{:03}{:03}".format(int(item.split("_")[0]), int(item.split("_")[1])) for item in path_row_list]

    if isinstance(scenes_to_filter_list, Path):
        with open(scenes_to_filter_list, "r") as fid:
            scenes_to_filter_list = [line.rstrip() for line in fid.readlines()]

    ls8_list, ls7_list, ls5_list = [], [], []

    LOGGER.info(SUMMARY, scenes_list_interuim=len(scenes_to_filter_list))
    for scene_path in scenes_to_filter_list:
        scene = os.path.basename(scene_path)

        try:
            path_row = scene.split("_")[2]
        except IndexError:
            kwargs = {DATASETPATH: scene_path, REASON: "Bad scene format", MSG: ("Bad scene format %s" % scene)}
            LOGGER.warn(SCENEREMOVED, **kwargs)

            continue

        if path_row not in path_row_list:
            kwargs = {DATASETPATH: scene_path, REASON: "Path row not in AOI", MSG: ("Path row %s" % path_row)}
            LOGGER.debug(SCENEREMOVED, **kwargs)
            continue

        if re.match(L8_PATTERN, scene):
            ls8_list.append(scene_path)

        elif re.match(L7_PATTERN, scene):
            ls7_list.append(scene_path)

        elif re.match(L5_PATTERN, scene):
            ls5_list.append(scene_path)

        else:
            kwargs = {DATASETPATH: scene_path, REASON: "Processing level too low"}
            LOGGER.debug(SCENEREMOVED, **kwargs)
    # SUMMARY
    LOGGER.info(SUMMARY, max_ls8_scenes=len(ls8_list))
    LOGGER.info(SUMMARY, max_ls7_scenes=len(ls7_list))
    LOGGER.info(SUMMARY, max_ls5_scenes=len(ls5_list))
    all_scenes_list = ls5_list + ls7_list + ls8_list
    scenes_list = all_scenes_list[:scene_limit]
    overflow_scenes_list = all_scenes_list[scene_limit:]
    LOGGER.info(SUMMARY, all_scenes=len(scenes_list))
    for scene in scenes_list:
        kwargs = {DATASETPATH: scene}
        LOGGER.info(SCENEADDED, **kwargs)
    for scene in overflow_scenes_list:
        kwargs = {DATASETPATH: scene, REASON: "Scene limit reached."}
        LOGGER.info(SCENEREMOVED, **kwargs)

    if out_dir is None:
        out_dir = Path.cwd()
    scenes_filepath = out_dir.joinpath("scenes_to_ARD_process.txt")
    write(scenes_filepath, scenes_list)
    return scenes_filepath, scenes_list


def mgrs_filter(scenes_to_filter_list: Union[List[str], Path], mgrs_list: Union[List[str], Path]) -> None:
    """Checks scenes to filter list if mrgs tile name are in mrgs list."""
    raise NotImplementedError


def process_scene(dataset, ancillary_ob, days_delta):
    if not dataset.local_path:
        kwargs = {
            DATASETID: str(dataset.id),
            REASON: "Skipping dataset without local paths",
            MSG: ("Bad scene format %s" % scene),
        }
        LOGGER.warning(SCENEREMOVED, **kwargs)
        return False

    assert dataset.local_path.name.endswith("metadata.yaml")

    days_ago = datetime.now(dataset.time.end.tzinfo) - timedelta(days=days_delta)
    # Continue here if a maturity level of final cannot be procduced
    # since the ancillary files are not there
    ancill_there, msg = ancillary_ob.definitive_ancillary_files(dataset.time.end)
    if ancill_there is False:
        file_path = (
            dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix()
        )
        kwargs = {
            DATASETPATH: file_path,
            SCENEID: dataset.metadata.landsat_scene_id,
            REASON: "ancillary files not ready",
            MSG: ("Not ready: %s" % msg),
        }
        LOGGER.info(SCENEREMOVED, **kwargs)
        return False

    if days_ago < dataset.time.end:
        file_path = (
            dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix()
        )
        kwargs = {
            DATASETPATH: file_path,
            REASON: "Not processing recent data",
            MSG: ("Not processing data after time delta(days:%d, Date %s)" % days_delta, days_ago.strftime("%Y-%m-%d")),
        }
        LOGGER.info(SCENEREMOVED, **kwargs)
        return False

    return True


def dataset_with_child(dc, dataset):
    """
    If any child exists that isn't archived
    :param dc:
    :param dataset:
    :return:
    """
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


def _do_parent_search(dc, product, brdfdir: Path, wvdir: Path, days_delta=0):
    # FIXME add expressions for more control
    if product in ARD_PARENT_PRODUCT_MAPPING:
        processed_ard_scene_ids = {
            result.landsat_scene_id
            for result in dc.index.datasets.search_returning(
                ("landsat_scene_id",), product=ARD_PARENT_PRODUCT_MAPPING[product]
            )
        }
        processed_ard_scene_ids = {chopped_scene_id(s) for s in processed_ard_scene_ids}
    else:
        # scene select has its own mapping for l1 product to ard product
        # (ARD_PARENT_PRODUCT_MAPPING).
        # If there is a l1 product that is not in this mapping this warning
        # is logged.
        # Scene select uses the l1 product to ard mapping to filter out
        # updated l1 scenes that have been processed using the old l1 scene.
        processed_ard_scene_ids = None
        LOGGER.warning("THE ARD ODC product name after ARD processing for %s is not known.", product)

    ancillary_ob = AncillaryFiles(brdf_dir=brdfdir, water_vapour_dir=wvdir)
    for dataset in dc.index.datasets.search(product=product):
        file_path = (
            dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix()
        )
        if processed_ard_scene_ids:
            if chopped_scene_id(dataset.metadata.landsat_scene_id) in processed_ard_scene_ids:
                kwargs = {
                    DATASETPATH: file_path,
                    REASON: "The scene has been processed",
                    SCENEID: dataset.metadata.landsat_scene_id,
                }
                LOGGER.debug(SCENEREMOVED, **kwargs)
                continue

        if process_scene(dataset, ancillary_ob, days_delta) is False:
            continue

        # If any child exists that isn't archived
        if dataset_with_child(dc, dataset):
            kwargs = {
                DATASETPATH: file_path,
                REASON: "Skipping dataset with children",
                SCENEID: dataset.metadata.landsat_scene_id,
            }
            LOGGER.debug(SCENEREMOVED, **kwargs)
            continue

        yield file_path


def get_landsat_level1_from_datacube_childless(
    outfile: Path, products: List[str], brdfdir: Path, wvdir: Path, config: Optional[Path] = None, days_delta: int = 21
) -> None:
    """Writes all the files returned from datacube for level1 to a text file."""
    dc = datacube.Datacube(app="gen-list", config=config)
    with open(outfile, "w") as fid:
        for product in products:
            for fp in _do_parent_search(dc, product, brdfdir=brdfdir, wvdir=wvdir, days_delta=days_delta):
                fid.write(fp + "\n")


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


def get_landsat_level1_from_datacube(outfile: Path, products: List[str], config: Optional[Path] = None) -> None:
    """Writes all the files returned from datacube for level1 to a text file."""

    # fixme add conf to the datacube API
    dc = datacube.Datacube(app="gen-list", config=config)
    with open(outfile, "w") as fid:
        for product in products:
            results = [
                item.local_path.parent.joinpath(item.metadata.landsat_product_id).with_suffix(".tar").as_posix()
                for item in dc.index.datasets.search(product=product)
            ]
            for fp in results:
                fid.write(fp + "\n")


def get_landsat_level1_file_paths(nci_dir: Path, out_file: Path, nprocs: Optional[int] = 1) -> None:
    """Write all the files with *.tar in nci_dir to a text file."""

    # this returns only folder name with PPP_RRR as is in NCI landsat archive
    nci_path_row_dirs = [
        nci_dir.joinpath(item) for item in nci_dir.iterdir() if re.match(r"[0-9]{3}_[0-9]{3}", item.name)
    ]

    # file paths searched using multiple threads
    with open(out_file, "w") as fid:
        with concurrent.futures.ThreadPoolExecutor(max_workers=nprocs) as executor:
            results = [
                executor.submit(lambda x: [fp.as_posix() for fp in x.glob("**/*.tar")], path_row)
                for path_row in nci_path_row_dirs
            ]
            for pt_list in concurrent.futures.as_completed(results):
                for _fp in pt_list.result():
                    fid.write(_fp + "\n")


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
    "--search-datacube", type=bool, help="whether query level1 files form database or file systems", default=True
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(LANDSAT_AOI_FILE),
    help="full path to a text files containing path/row or MGRS tile name to act as a filter",
)
@click.option(
    "--nprocs", type=int, help="number of processes to enable faster search through a  large file system", default=1
)
@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a datacube config text file",
    default=None,
)
@click.option("--days_delta", type=int, help="Only process files older than days delta.", default=0)
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
# This isn't being used, so I'm taking it out
# aerosol_shapefile: click.Path=AEROSOLSHAPEFILE,
@LogMainFunction()
def scene_select(
    usgs_level1_files: click.Path,
    search_datacube: bool,
    allowed_codes: click.Path,
    nprocs: int,
    config: click.Path,
    days_delta: int,
    products: list,
    logdir: click.Path,
    brdfdir: click.Path,
    wvdir: click.Path,
    stop_logging: bool,
    log_config: click.Path,
    scene_limit: Optional[int],
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
    logdir = Path(logdir).resolve()
    brdfdir = Path(brdfdir).resolve()
    wvdir = Path(wvdir).resolve()
    # If we write a file we write it in the job dir
    # set up the scene select job dir in the log dir
    jobid = uuid.uuid4().hex[0:6]
    jobdir = logdir.joinpath(FMT2.format(jobid=jobid))
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
        if search_datacube:
            get_landsat_level1_from_datacube_childless(
                usgs_level1_files,
                products=products,
                brdfdir=brdfdir,
                wvdir=wvdir,
                config=config,
                days_delta=days_delta,
            )
        else:
            LOGGER.warning("searching the file system is untested.")

            get_landsat_level1_file_paths(
                Path("/g/data/da82/AODH/USGS/L1/Landsat/C1/"), usgs_level1_files, nprocs=nprocs
            )
    # there is a usgs_level1_files file with initial selected scenes

    # apply path_row filter and
    # processing level filtering
    scenes_filepath, all_scenes_list = path_row_filter(
        Path(usgs_level1_files),
        Path(allowed_codes) if isinstance(allowed_codes, str) else allowed_codes,
        jobdir,
        scene_limit=scene_limit,
    )

    _calc_node_with_defaults(ard_click_params, len(all_scenes_list))

    # write pbs script
    run_ard_pathfile = jobdir.joinpath("run_ard_pbs.sh")
    with open(run_ard_pathfile, "w") as src:
        src.write(make_ard_pbs(scenes_filepath, **ard_click_params))

    # Make the script executable
    os.chmod(run_ard_pathfile, os.stat(run_ard_pathfile).st_mode | stat.S_IEXEC)

    # run the script
    if run_ard is True:
        subprocess.run([run_ard_pathfile], check=True)

    LOGGER.info("info", jobdir=str(jobdir))
    print("Job directory: " + str(jobdir))

    return scenes_filepath, all_scenes_list


if __name__ == "__main__":
    scene_select()
