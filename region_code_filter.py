#!/usr/bin/env python3


import os
import logging
from pathlib import Path
from typing import List, Optional, Union
import re
import concurrent.futures

import click
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import cascaded_union

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


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
    """ Code to read a shapefile and return its content as a geopandas dataframe"""
    return gpd.read_file(str(shapefile))


def _get_auxiliary_extent(
    gpd_df: gpd.GeoDataFrame, subset_key: Optional[str] = None
) -> Polygon:
    """ Returns the extent of all auxiliary dataset or an extent by a subset_key"""
    if subset_key:
        return cascaded_union(
            [geom for geom in gpd_df[gpd_df.auxiliary_ == subset_key].geometry]
        )
    return cascaded_union([geom for geom in gpd_df.geometry])


def _auxiliary_overlap_extent(_extents: List[Polygon]) -> Polygon:
    """ Returns the overlaped regions from list of extents derived from auxiliary datasets"""

    overlap_extent = _extents[0]
    for idx, extent in enumerate(_extents, start=1):
        overlap_extent = overlap_extent.intersection(extent)

    return overlap_extent


def nbar_scene_filter(
    nbar_auxiliary_extent: Polygon, df_scenes_to_filter: Union[gpd.GeoDataFrame, Path]
) -> List[str]:
    """ Filtering method to check if acquisition can be used for nbar processing"""

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
    """ processing block for nbar scene filter"""

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
    """ helper method to write contents in a list to a file"""
    with open(filename, "w") as fid:
        for item in list_to_write:
            fid.write(item + "\n")


def path_row_filter(
    scenes_to_filter_list: Union[List[str], Path], path_row_list: Union[List[str], Path]
) -> None:
    """Filter scenes to check if path/row of a scene is allowed in a path row list"""

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

    ls8_list, ls7_list, ls5_list = [], [], []

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

        if re.match(L8_PATTERN, scene):
            ls8_list.append(scene_path)

        elif re.match(L7_PATTERN, scene):
            ls7_list.append(scene_path)

        elif re.match(L5_PATTERN, scene):
            ls5_list.append(scene_path)

        else:
            _LOG.info(scene_path)

    out_dir = Path.cwd()
    _write(out_dir.joinpath("L08_CollectionUpgrade_Level1_list.txt"), ls8_list)
    _write(out_dir.joinpath("L07_CollectionUpgrade_Level1_list.txt"), ls7_list)
    _write(out_dir.joinpath("L05_CollectionUpgrade_Level1_list.txt"), ls5_list)


def mgrs_filter(
    scenes_to_filter_list: Union[List[str], Path], mgrs_list: Union[List[str], Path]
) -> None:
    """checks scenes to filter list if mrgs tile name are in mrgs list """
    raise NotImplementedError


def get_landsat_level1_file_paths(
    nci_dir: Path, out_file: Path, nprocs: Optional[int] = 1
) -> None:
    """ Write all the files with *.tar in nci_dir to a text file"""

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


@click.command()
@click.option(
    "--brdf-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to brdf extent shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/brdf_tiles_new.shp",
)
@click.option(
    "--one-deg-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to one deg dsm version 1 extent shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-deg-dsm-v1.shp",
)
@click.option(
    "--one-sec-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to one sec dsm version 1 shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-sec-dsm-v1.shp",
)
@click.option(
    "--one-deg-dsm-v2-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to dsm shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-deg-dsm-v2.shp",
)
@click.option(
    "--aerosol-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to aerosol shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/aerosol.shp",
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
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/global_wrs_mgrs_shps/wrsdall_Decending.shp",
)
@click.option(
    "--world-mgrs-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to global mgrs shapefile",
    default="/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/global_wrs_mgrs_shps/S2_tile.shp",
)
@click.option(
    "--usgs-level1-files",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to a text files containing all the level-1 usgs/esa  list to be filtered",
)
@click.option(
    "--allowed-codes",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to a text files containing path/row or MGRS tile name to act as a filter",
)
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
    allowed_codes: click.Path,
):
    if not usgs_level1_files:
        get_landsat_level1_file_paths(
            Path("/g/data/da82/AODH/USGS/L1/Landsat/C1"),
            Path(
                "/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/all_landsat_scenes.txt"
            ),
            nprocs=8,
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

    path_row_filter(
        Path(usgs_level1_files),
        Path(allowed_codes) if isinstance(allowed_codes, str) else allowed_codes,
    )


if __name__ == "__main__":
    logging.basicConfig(filename="ignored_scenes_list.log", level=logging.INFO)
    main()
