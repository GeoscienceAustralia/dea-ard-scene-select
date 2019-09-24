#!/usr/bin/env python3


import os
import logging
from pathlib import Path
from typing import Optional, List, Union
import click

import pandas as pd
import geopandas as gpd
import shapely.wkt
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union


_LOG = logging.getLogger(__name__)


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
    """ Code to read a shapefile and return its content as a geopandas dataframe"""
    return gpd.read_file(shapefile)


def _get_auxiliary_extent(gpd_df: Path, subset_key: Optional[str] = None) -> Polygon:
    """ Returns the extent of all auxiliary dataset or an extent by a subset_key"""
    if subset_key:
        return cascaded_union([geom for geom in gpd_df[gpd_df.auxiliary_ == subset_key].geometry])
    return cascaded_union([geom for geom in gpd_df.geometry])


def _auxiliary_overlap_extent(_extents: List[Polygon]) -> Polygon:
    """ Returns the overlaped regions from list of extents derived from auxiliary datasets"""

    overlap_extent = _extents[0]
    for idx, extent in enumerate(_extents, start=1):
        overlap_extent = overlap_extent.intersection(extent)

    return overlap_extent


def path_row_filter(scene_to_filter_list, path_row_list: List[str]) -> None:
    """Filter scenes to check if path/row of a scene is allowed in a path row list"""
    for path_row in path_row_list:
        print(path_row)


def nbar_scene_filter(
    nbar_auxiliary_extent: Polygon,
    df_scenes_to_filter: Union[gpd.GeoDataFrame, Path]
) -> List[str]:
    """ Filtering method to check if acquisition can be used for nbar processing"""

    if isinstance(df_scenes_to_filter, Path):
        df_scenes_to_filter = read_shapefile(df_scenes_to_filter)
        print('yes')

    # initial filter is to check if scene intersects with auxiliary data extent
    aux_overlaped_gdf = gpd.GeoDataFrame()
    for idx, geom in enumerate(df_scenes_to_filter.geometry):
        if geom.intersects(nbar_auxiliary_extent):
            aux_overlaped_gdf = aux_overlaped_gdf.append(df_scenes_to_filter.iloc[idx])

    # aux_overlaped_gdf.to_file('/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/test_outputs/overlapping_auxiliary_extent_no_aerosol.shp', driver="ESRI Shapefile")
    #TODO additional check over ocean mask extent to see if scene is all offshore of inland
    return aux_overlaped_gdf


def global_wrs_to_ga_extent(wrs_data: Path, aux_extents_vectorfiles: List[Path]) -> List[str]:
    """ processing block for nbar scene filter"""

    # get extents from auxiliary vector files
    extents = [_get_auxiliary_extent(read_shapefile(fp)) for fp in aux_extents_vectorfiles]

    nbar_extent = _auxiliary_overlap_extent(extents)
    # filter global wrs data
    scenes_df = nbar_scene_filter(nbar_extent, wrs_data)

    return list(scenes_df.PATH_ROW.values)


@click.command()
@click.option(
    '--brdf-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to brdf extent shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/brdf_tiles_new.shp'
)
@click.option(
    '--one-deg-dsm-v1-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to one deg dsm version 1 extent shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-deg-dsm-v1.shp'
)

@click.option(
    '--one-sec-dsm-v1-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to one sec dsm version 1 shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-sec-dsm-v1.shp'
)
@click.option(
    '--one-deg-dsm-v2-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to dsm shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-deg-dsm-v2.shp'
)
@click.option(
    '--aerosol-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to aerosol shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/aerosol.shp'
)
@click.option(
    '--world-wrs-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to global wrs shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/global_wrs_mgrs_shps/wrsdall_Decending.shp'
)
@click.option(
    '--world-mgrs-shapefile',
    type=click.Path(dir_okay=False, file_okay=True),
    help='full path to global mgrs shapefile',
    default='/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/global_wrs_mgrs_shps/S2_tile.shp'
)
def main(
    brdf_shapefile: click.Path,
    one_deg_dsm_v1_shapefile: click.Path,
    one_sec_dsm_v1_shapefile: click.Path,
    one_deg_dsm_v2_shapefile: click.Path,
    aerosol_shapefile: click.Path,
    world_wrs_shapefile: click.Path,
    world_mgrs_shapefile: click.Path
):
    wrs_list = global_wrs_to_ga_extent(Path(world_wrs_shapefile),
                                       [brdf_shapefile,
                                       one_deg_dsm_v1_shapefile,
                                       one_sec_dsm_v1_shapefile,
                                       one_deg_dsm_v2_shapefile
                                       ]
               )

    #TODO write level1 yaml file to filter the scenes to wrs in the wrs list
    print(wrs_list)

if __name__ == '__main__':
    main()

