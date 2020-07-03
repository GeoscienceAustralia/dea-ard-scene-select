#!/usr/bin/env python3

import logging
from pathlib import Path
from typing import List, Optional, Union
import uuid
import click
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import cascaded_union


EXTENT_DIR = Path(__file__).parent.joinpath("auxiliary_extents")
GLOBAL_MGRS_WRS_DIR = Path(__file__).parent.joinpath("global_wrs_mgrs_shps")
DATA_DIR = Path(__file__).parent.joinpath("data")
LOG_FILE = "generate_aoi.log"
AOI_FILE = "australian-aoi.txt"

BRDFSHAPEFILE = EXTENT_DIR.joinpath("brdf_tiles_new.shp")
ONEDEGDSMV1SHAPEFILE = EXTENT_DIR.joinpath("one-deg-dsm-v1.shp")
ONESECDSMV1SHAPEFILE = EXTENT_DIR.joinpath("one-sec-dsm-v1.shp")
ONEDEGDSMV2SHAPEFILE = EXTENT_DIR.joinpath("one-deg-dsm-v2.shp")
AEROSOLSHAPEFILE = EXTENT_DIR.joinpath("aerosol.shp")
WRSSHAPEFILE = GLOBAL_MGRS_WRS_DIR.joinpath("wrsdall_Decending.shp")
MGRSSHAPEFILE = GLOBAL_MGRS_WRS_DIR.joinpath("S2_tile.shp")
DEF_SAT_PROVIDER = "USGS"
FMT2 = "generte-aoi-jobid-{jobid}"


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
    """Code to read a shapefile and return its content as a geopandas dataframe"""
    return gpd.read_file(str(shapefile))


def _get_auxiliary_extent(gpd_df: gpd.GeoDataFrame, subset_key: Optional[str] = None) -> Polygon:
    """Returns the extent of all auxiliary dataset or an extent by a subset_key"""
    if subset_key:
        return cascaded_union([geom for geom in gpd_df[gpd_df.auxiliary_ == subset_key].geometry])
    return cascaded_union([geom for geom in gpd_df.geometry])


def _auxiliary_overlap_extent(_extents: List[Polygon]) -> Polygon:
    """Returns the overlaped regions from list of extents derived from auxiliary datasets."""

    overlap_extent = _extents[0]
    for idx, extent in enumerate(_extents, start=1):
        overlap_extent = overlap_extent.intersection(extent)

    return overlap_extent


def nbar_scene_filter(nbar_auxiliary_extent: Polygon, df_scenes_to_filter: Union[gpd.GeoDataFrame, Path]) -> List[str]:
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
    _global_tiles_data: Path, aux_extents_vectorfiles: List[Path], _satellite_data_provider: str
) -> List[str]:
    """Processing block for nbar scene filter."""

    # get extents from auxiliary vector files
    extents = [_get_auxiliary_extent(read_shapefile(fp)) for fp in aux_extents_vectorfiles]
    nbar_aux_extent = _auxiliary_overlap_extent(extents)

    # filter global tile data to nbar_aux_extent
    scenes_df = nbar_scene_filter(nbar_aux_extent, _global_tiles_data)

    if _satellite_data_provider == "USGS":
        return list(scenes_df.PATH_ROW.values)

    return list(scenes_df.Name.values)


@click.command()
@click.option(
    "--brdf-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to brdf extent shapefile",
    default=BRDFSHAPEFILE,
)
@click.option(
    "--one-deg-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True),
    help="full path to one deg dsm version 1 extent shapefile",
    default=ONEDEGDSMV1SHAPEFILE,
)
@click.option(
    "--one-sec-dsm-v1-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to one sec dsm version 1 shapefile",
    default=ONESECDSMV1SHAPEFILE,
)
@click.option(
    "--one-deg-dsm-v2-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to dsm shapefile",
    default=ONEDEGDSMV2SHAPEFILE,
)
@click.option(
    "--satellite-data-provider",
    type=click.Choice(["ESA", "USGS"]),
    help="satellite data provider (ESA or USGS)",
    default=DEF_SAT_PROVIDER,
)
@click.option(
    "--world-wrs-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to global wrs shapefile",
    default=WRSSHAPEFILE,
)
@click.option(
    "--world-mgrs-shapefile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    help="full path to global mgrs shapefile",
    default=MGRSSHAPEFILE,
)
@click.option(
    "--workdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output working directory.",
    default=Path.cwd(),
)
def generate_region(
    workdir: click.Path,
    satellite_data_provider: str = DEF_SAT_PROVIDER,
    brdf_shapefile: click.Path = BRDFSHAPEFILE,
    one_deg_dsm_v1_shapefile: click.Path = ONEDEGDSMV1SHAPEFILE,
    one_sec_dsm_v1_shapefile: click.Path = ONESECDSMV1SHAPEFILE,
    one_deg_dsm_v2_shapefile: click.Path = ONEDEGDSMV2SHAPEFILE,
    world_wrs_shapefile: click.Path = WRSSHAPEFILE,
    world_mgrs_shapefile: click.Path = MGRSSHAPEFILE,
):
    """

    :return: list of scenes to ARD process
    """
    workdir = Path(workdir).resolve()
    # set up the scene select job dir in the work dir
    jobid = uuid.uuid4().hex[0:6]
    jobdir = workdir.joinpath(FMT2.format(jobid=jobid))
    jobdir.mkdir(exist_ok=True)
    #
    print("Job directory: " + str(jobdir))
    log_filepath = jobdir.joinpath(LOG_FILE)
    logging.basicConfig(filename=log_filepath, level=logging.INFO)  # INFO

    # needed build the allowed_codes using the shapefiles
    _extent_list = [brdf_shapefile, one_deg_dsm_v1_shapefile, one_sec_dsm_v1_shapefile, one_deg_dsm_v2_shapefile]
    global_tiles_data = Path(world_wrs_shapefile)
    if satellite_data_provider == "ESA":
        global_tiles_data = Path(world_mgrs_shapefile)
    allowed_codes = subset_global_tiles_to_ga_extent(global_tiles_data, _extent_list, satellite_data_provider)
    # AOI_FILE
    aoi_filepath = jobdir.joinpath(AOI_FILE)
    with open(aoi_filepath, "w") as f:
        for item in allowed_codes:
            f.write("%s\n" % item)
    return aoi_filepath, allowed_codes  # This is used for testing


if __name__ == "__main__":
    generate_region()
