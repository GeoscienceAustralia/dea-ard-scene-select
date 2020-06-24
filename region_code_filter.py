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
import click
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import cascaded_union
from datetime import datetime, timedelta

import datacube


EXTENT_DIR = Path(__file__).parent.joinpath("auxiliary_extents")
GLOBAL_MGRS_WRS_DIR = Path(__file__).parent.joinpath("global_wrs_mgrs_shps")
DATA_DIR = Path(__file__).parent.joinpath("data")
ODC_FILTERED_FILE = "DataCube_all_landsat_scenes.txt"
LOG_FILE = "ignored_scenes_list.log"
PRODUCTS = '["ga_ls5t_level1_3", "ga_ls7e_level1_3", \
                    "usgs_ls5t_level1_1", "usgs_ls7e_level1_1", "usgs_ls8c_level1_1"]'
FMT2 = 'jobid-{jobid}'

ARD_PARENT_PRODUCT_MAPPING =  {"ga_ls5t_level1_3": "ga_ls5t_ard_3",
                               "ga_ls7e_level1_3": "ga_ls7e_ard_3",
                               "ga_ls8c_level1_3": "ga_ls8c_ard_3",
                               "usgs_ls5t_level1_1": "ga_ls5t_ard_3",
                               "usgs_ls7e_level1_1": "ga_ls7e_ard_3",
                               "usgs_ls8c_level1_1": "ga_ls8c_ard_3"
                               }

NODE_TEMPLATE = ("""#!/bin/bash
module unload dea
source {env}

ard_pbs --level1-list {scene_list} {ard_args}
""")

_LOG = logging.getLogger(__name__)

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

# landsat 8 filename pattern is configured to match only
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
    """  """
    def type_cast_value(self, ctx, value):
        try:
            value = str(value)
            assert value.count('[') == 1 and value.count(']') == 1
            list_as_str = value.replace('"', "'").split('[')[1].split(']')[0]
            list_of_items = [item.strip().strip("'") for item in list_as_str.split(',')]
            return list_of_items
        except Exception:
            raise click.BadParameter(value)


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
    """Code to read a shapefile and return its content as a geopandas dataframe"""
    return gpd.read_file(str(shapefile))


def _get_auxiliary_extent(
    gpd_df: gpd.GeoDataFrame, subset_key: Optional[str] = None
) -> Polygon:
    """Returns the extent of all auxiliary dataset or an extent by a subset_key"""
    if subset_key:
        return cascaded_union(
            [geom for geom in gpd_df[gpd_df.auxiliary_ == subset_key].geometry]
        )
    return cascaded_union([geom for geom in gpd_df.geometry])


def _auxiliary_overlap_extent(_extents: List[Polygon]) -> Polygon:
    """Returns the overlaped regions from list of extents derived from auxiliary datasets."""

    overlap_extent = _extents[0]
    for idx, extent in enumerate(_extents, start=1):
        overlap_extent = overlap_extent.intersection(extent)

    return overlap_extent


def nbar_scene_filter(
    nbar_auxiliary_extent: Polygon, df_scenes_to_filter: Union[gpd.GeoDataFrame, Path]
) -> List[str]:
    """Filtering method to check if acquisition can be used for nbar processing."""

    if isinstance(df_scenes_to_filter, Path):
        df_scenes_to_filter = read_shapefile(df_scenes_to_filter)

    # initial filter is to check if scene intersects with auxiliary data extent
    aux_overlaped_gdf = gpd.GeoDataFrame()
    for idx, geom in enumerate(df_scenes_to_filter.geometry):
        if geom.intersects(nbar_auxiliary_extent):
            aux_overlaped_gdf = aux_overlaped_gdf.append(df_scenes_to_filter.iloc[idx])

    return aux_overlaped_gdf


def subset_global_tiles_to_ga_extent(
    _global_tiles_data: Path,
    aux_extents_vectorfiles: List[Path],
    _satellite_data_provider: str,
) -> List[str]:
    """Processing block for nbar scene filter."""

    # get extents from auxiliary vector files
    extents = [
        _get_auxiliary_extent(read_shapefile(fp)) for fp in aux_extents_vectorfiles
    ]
    nbar_aux_extent = _auxiliary_overlap_extent(extents)

    # filter global tile data to nbar_aux_extent
    scenes_df = nbar_scene_filter(nbar_aux_extent, _global_tiles_data)

    if _satellite_data_provider == "USGS":
        return list(scenes_df.PATH_ROW.values)

    return list(scenes_df.Name.values)


def _write(filename: Path, list_to_write: List) -> None:
    """A helper method to write contents in a list to a file."""
    with open(filename, "w") as fid:
        for item in list_to_write:
            fid.write(item + "\n")


def path_row_filter(
    scenes_to_filter_list: Union[List[str], Path],
    path_row_list: Union[List[str], Path],
    out_dir: Optional[Path] = None,
) -> None:
    """Filter scenes to check if path/row of a scene is allowed in a path row list."""

    if isinstance(path_row_list, Path):
        with open(path_row_list, "r") as fid:
            path_row_list = [line.rstrip() for line in fid.readlines()]

    path_row_list = [
        "{:03}{:03}".format(int(item.split("_")[0]), int(item.split("_")[1]))
        for item in path_row_list
    ]
    if isinstance(scenes_to_filter_list, Path):
        with open(scenes_to_filter_list, "r") as fid:
            scenes_to_filter_list = [line.rstrip() for line in fid.readlines()]

    ls8_list, ls7_list, ls5_list, to_process = [], [], [], []

    for scene_path in scenes_to_filter_list:
        scene = os.path.basename(scene_path)

        try:
            path_row = scene.split("_")[2]
        except IndexError:
            _LOG.info(scene_path)
            continue

        if path_row not in path_row_list:
            _LOG.info(scene_path)
            continue

        to_process.append(scene_path)

        if re.match(L8_PATTERN, scene):
            ls8_list.append(scene_path)

        elif re.match(L7_PATTERN, scene):
            ls7_list.append(scene_path)

        elif re.match(L5_PATTERN, scene):
            ls5_list.append(scene_path)

        else:
            _LOG.info(scene_path)
    all_scenes_list = ls5_list + ls7_list + ls8_list
    count_all_scenes_list = len(all_scenes_list)
    if out_dir is None:
        out_dir = Path.cwd()
    scenes_filepath = out_dir.joinpath("scenes_to_ARD_process.txt")
    _write(out_dir.joinpath("DataCube_L08_Level1.txt"), ls8_list)
    _write(out_dir.joinpath("DataCube_L07_Level1.txt"), ls7_list)
    _write(out_dir.joinpath("DataCube_L05_Level1.txt"), ls5_list)
    _write(out_dir.joinpath("no_file_pattern_matching.txt"), to_process)
    _write(scenes_filepath, all_scenes_list)
    return scenes_filepath, count_all_scenes_list


def mgrs_filter(
    scenes_to_filter_list: Union[List[str], Path], mgrs_list: Union[List[str], Path]
) -> None:
    """Checks scenes to filter list if mrgs tile name are in mrgs list."""
    raise NotImplementedError


def process_scene(dataset, days_delta):
    if not dataset.local_path:
        _LOG.warning("Skipping dataset without local paths: %s", dataset.id)
        return False

    assert dataset.local_path.name.endswith("metadata.yaml")

    days_ago = datetime.now(dataset.time.end.tzinfo) - timedelta(days=days_delta)
    if days_ago < dataset.time.end:
        file_path = dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(
            ".tar").as_posix()
        _LOG.info("%s #Skipping dataset after time delta(days:%d, Date %s): %s",
                  file_path,
                  days_delta,
                     days_ago.strftime('%Y-%m-%d'), dataset.id)
        return False

    return True

def dataset_with_child(dc, dataset):
    """
    If any child exists that isn't archived
    :param dc:
    :param dataset:
    :return:
    """
    return any(not child_dataset.is_archived
                for child_dataset in dc.index.datasets.get_derived(dataset.id)
            )


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


def _do_parent_search(dc, product, days_delta=0):
    #FIXME add expressions for more control
    if product in ARD_PARENT_PRODUCT_MAPPING:
        processed_ard_scene_ids = {
            result.landsat_scene_id for result in
            dc.index.datasets.search_returning(
                ('landsat_scene_id',),
                product=ARD_PARENT_PRODUCT_MAPPING[product])
        }
        processed_ard_scene_ids = {chopped_scene_id(s) for s in processed_ard_scene_ids}
    else:
        processed_ard_scene_ids = None
        _LOG.info(
           "Child ARD not know for product: (%s)", product
        )

    for dataset in dc.index.datasets.search(product=product):
        file_path = dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(
            ".tar").as_posix()
        if processed_ard_scene_ids:
            if chopped_scene_id(dataset.metadata.landsat_scene_id) in processed_ard_scene_ids:
                _LOG.info(
                   "%s # Skipping dataset since scene id in ARD: (%s)", file_path, dataset.id
                )
                continue

        if process_scene(dataset, days_delta) is False:
            continue

        # If any child exists that isn't archived
        if dataset_with_child(dc, dataset):
            # Name of input folder treated as telemetry dataset name
            name = dataset.local_path.parent.name
            _LOG.info(
                "%s # Skipping dataset with children: (%s)", file_path, dataset.id
            )
            continue

        yield file_path

def get_landsat_level1_from_datacube_childless(
    outfile: Path,
    products: List[str],
    config: Optional[Path] = None,
    days_delta: int = 21,
) -> None:
    """Writes all the files returned from datacube for level1 to a text file."""
    dc = datacube.Datacube(app="gen-list", config=config)
    with open(outfile, "w") as fid:
        for product in products:
            for fp in _do_parent_search(dc, product, days_delta=days_delta):
                fid.write(fp + "\n")

                
def _calc_nodes_req(granule_count, walltime, workers, hours_per_granule=1.5):
    """ Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59', 28)
    2
    >>> _calc_nodes_req(800, '20:00', 28)
    3
    """
    print(granule_count)
    print(walltime)
    print(workers)
    hours, _, _ = [int(x) for x in walltime.split(':')]
    # to avoid divide by zero errors
    if hours == 0:
        hours = 1
    nodes = int(math.ceil(float(hours_per_granule * granule_count) \
                          / (hours * workers)))
    return nodes


def get_landsat_level1_from_datacube(
    outfile: Path,
    products: List[str],
    config: Optional[Path] = None,
) -> None:
    """Writes all the files returned from datacube for level1 to a text file."""
    #fixme add conf to the datacube API
    dc = datacube.Datacube(app="gen-list", config=config)
    with open(outfile, "w") as fid:
        for product in products:
            results = [
                item.local_path.parent.joinpath(item.metadata.landsat_product_id)
                .with_suffix(".tar")
                .as_posix()
                for item in dc.index.datasets.search(product=product)
            ]
            for fp in results:
                fid.write(fp + "\n")

def get_landsat_level1_file_paths(
    nci_dir: Path, out_file: Path, nprocs: Optional[int] = 1
) -> None:
    """Write all the files with *.tar in nci_dir to a text file."""

    # this returns only folder name with PPP_RRR as is in NCI landsat archive
    nci_path_row_dirs = [
        nci_dir.joinpath(item)
        for item in nci_dir.iterdir()
        if re.match(r"[0-9]{3}_[0-9]{3}", item.name)
    ]

    # file paths searched using multiple threads
    with open(out_file, "w") as fid:
        with concurrent.futures.ThreadPoolExecutor(max_workers=nprocs) as executor:
            results = [
                executor.submit(
                    lambda x: [fp.as_posix() for fp in x.glob("**/*.tar")], path_row
                )
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
        ard_params.append("--" + key)
        # Make path strings absolute
        if key in ('logdir', 'pkgdir'):
            value = Path(value).resolve()
        ard_params.append(str(value))
    ard_arg_string = " ".join(ard_params)
    return ard_arg_string


def make_ard_pbd(**ard_click_params):
    level1_list = ard_click_params['level1_list']
    env = ard_click_params['env']

    # Use the template format to make sure 'level1_list' is there
    del ard_click_params['level1_list']

    ard_args_str = dict2ard_arg_string(ard_click_params)
    pbs = NODE_TEMPLATE.format(env=env,
                               scene_list=level1_list,
                               ard_args=ard_args_str)
    return pbs

@click.command()
@click.option(
    "--brdf-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to brdf extent shapefile",
    default=EXTENT_DIR.joinpath("brdf_tiles_new.shp"),
)
@click.option(
    "--one-deg-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to one deg dsm version 1 extent shapefile",
    default=EXTENT_DIR.joinpath("one-deg-dsm-v1.shp"),
)
@click.option(
    "--one-sec-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to one sec dsm version 1 shapefile",
    default=EXTENT_DIR.joinpath("one-sec-dsm-v1.shp"),
)
@click.option(
    "--one-deg-dsm-v2-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to dsm shapefile",
    default=EXTENT_DIR.joinpath("one-deg-dsm-v2.shp"),
)
@click.option(
    "--aerosol-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to aerosol shapefile",
    default=EXTENT_DIR.joinpath("aerosol.shp"),
)
@click.option(
    "--satellite-data-provider",
    type=click.Choice(["ESA", "USGS"]),
    help="satellite data provider (ESA or USGS)",
    default="USGS",
)
@click.option(
    "--world-wrs-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to global wrs shapefile",
    default=GLOBAL_MGRS_WRS_DIR.joinpath("wrsdall_Decending.shp"),
)
@click.option(
    "--world-mgrs-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to global mgrs shapefile",
    default=GLOBAL_MGRS_WRS_DIR.joinpath("S2_tile.shp"),
)
@click.option(
    "--usgs-level1-files",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a text files containing all the level-1 USGS/ESA list to be filtered",
)
@click.option(
    "--search-datacube",
    type=bool,
    help="whether query level1 files form database or file systems",
    default=True,
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to a text files containing path/row or MGRS tile name to act as a filter",
)
@click.option(
    "--nprocs",
    type=int,
    help="number of processes to enable faster search through a  large file system",
    default=1,
)
@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a datacube config text file",
    default=None
)
@click.option(
    "--days_delta",
    type=int,
    help="Only process files older than days delta.",
    default=14,
)
@click.option(
    "--products",
    cls=PythonLiteralOption,
    type=list,
    help="List the ODC products to be processed. e.g. \
    '[\"ga_ls5t_level1_3\", \"usgs_ls8c_level1_1\"]'",
    default=PRODUCTS
)
@click.option("--workdir", type=click.Path(file_okay=False, writable=True),
              help="The base output working directory.", default=Path.cwd())
@click.option("--run-ard", default=False, is_flag=True,
              help="Execute the ard_pbs script.")
## This are passed on to ard processing
@click.option("--test", default=False, is_flag=True,
              help="Test job execution (Don't submit the job to the "
                    "PBS queue).")
@click.option("--walltime",
              help="Job walltime in `hh:mm:ss` format.")
@click.option("--email",
              help="Notification email address.")
@click.option("--project", default="v10", help="Project code to run under.")
@click.option("--logdir", type=click.Path(file_okay=False, writable=True),
              help="The base logging and scripts output directory.")
@click.option("--pkgdir", type=click.Path(file_okay=False, writable=True),
              help="The base output packaged directory.")
@click.option("--env", type=click.Path(exists=True, readable=True),
              help="Environment script to source.")
@click.option("--workers", type=click.IntRange(1, 48),
              help="The number of workers to request per node.")
@click.option("--nodes", help="The number of nodes to request.")
@click.option("--memory",
              help="The memory in GB to request per node.")
@click.option("--jobfs",
              help="The jobfs memory in GB to request per node.")
def main(
        brdf_shapefile: click.Path,
        one_deg_dsm_v1_shapefile: click.Path,
        one_sec_dsm_v1_shapefile: click.Path,
        one_deg_dsm_v2_shapefile: click.Path,
        satellite_data_provider: str,
        aerosol_shapefile: click.Path,
        world_wrs_shapefile: click.Path,
        world_mgrs_shapefile: click.Path,
        usgs_level1_files: click.Path,
        search_datacube: bool,
        allowed_codes: click.Path,
        nprocs: int,
        config: click.Path,
        days_delta: int,
        products: list,
        workdir: click.Path,
        run_ard: bool,
        **ard_click_params: dict,
    ):
    """
    The keys for ard_click_params;
        test: bool,
        logdir: click.Path,
        pkgdir: click.Path,
        env: click.Path,
        ardworkers: int,
        ardnodes: int,
        ardmemory: int,
        ardjobfs: int,
        project: str,
        walltime: str,
        email: str
    :param brdf_shapefile:
    :param one_deg_dsm_v1_shapefile:
    :param one_sec_dsm_v1_shapefile:
    :param one_deg_dsm_v2_shapefile:
    :param satellite_data_provider:
    :param aerosol_shapefile:
    :param world_wrs_shapefile:
    :param world_mgrs_shapefile:
    :param usgs_level1_files:
    :param search_datacube:
    :param allowed_codes:
    :param nprocs:
    :param config:
    :param days_delta:
    :param products:
    :param workdir:
    :return:
    """
    workdir = Path(workdir).resolve()
    # set up the scene select job dir in the work dir
    jobid = uuid.uuid4().hex[0:6]
    jobdir = Path(os.path.join(workdir, FMT2.format(jobid=jobid)))

    #
    print("Job directory: " + str(jobdir))
    log_filepath = os.path.join(jobdir, LOG_FILE)
    if not os.path.exists(jobdir):
        os.makedirs(jobdir)
    logging.basicConfig(filename=log_filepath, level=logging.INFO) # INFO



    if not usgs_level1_files:
        usgs_level1_files = os.path.join(jobdir, ODC_FILTERED_FILE)
        if search_datacube:
            get_landsat_level1_from_datacube_childless(usgs_level1_files, config=config,
                                                       days_delta=days_delta,
                                                       products=products)
        else:
            _LOG.warning("searching the file system is untested.")

            get_landsat_level1_file_paths(
                Path("/g/data/da82/AODH/USGS/L1/Landsat/C1/"),
                usgs_level1_files,
                nprocs=nprocs,
            )

    if not allowed_codes:
        _extent_list = [
            brdf_shapefile,
            one_deg_dsm_v1_shapefile,
            one_sec_dsm_v1_shapefile,
            one_deg_dsm_v2_shapefile,
        ]
        global_tiles_data = Path(world_wrs_shapefile)
        if satellite_data_provider == "ESA":
            global_tiles_data = Path(world_mgrs_shapefile)
        allowed_codes = subset_global_tiles_to_ga_extent(
            global_tiles_data, _extent_list, satellite_data_provider
        )
    scenes_filepath, count_all_scenes_list = path_row_filter(
        Path(usgs_level1_files),
        Path(allowed_codes) if isinstance(allowed_codes, str) else allowed_codes,
        out_dir=jobdir,
    )

    # *********** Moving around

    # The workdir is used by ard_pbs
    ard_click_params['workdir'] = workdir
    ard_click_params['level1_list'] = scenes_filepath
    pbs_script_text = make_ard_pbd(**ard_click_params)

    if ard_click_params['nodes'] is None:
        if ard_click_params['walltime'] is None:
            walltime = "05:00:00"
        else:
            walltime = ard_click_params['walltime']
        if ard_click_params['workers'] is None:
            workers = 30
        else:
            workers = ard_click_params['workers']
        ard_click_params['nodes'] = _calc_nodes_req(count_all_scenes_list,
                                                    walltime, workers)

    # write pbs script
    run_ard_pathfile = os.path.join(jobdir, "run_ard_pbs.sh") 
    with open(run_ard_pathfile, 'w') as src:
        src.write(pbs_script_text)

    # Make the script executable
    st = os.stat(run_ard_pathfile)
    os.chmod(run_ard_pathfile, st.st_mode | stat.S_IEXEC)
    # *********** Moving around

    if run_ard is True:
        subprocess.run([run_ard_pathfile])

if __name__ == "__main__":
    main()
