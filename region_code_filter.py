#!/usr/bin/env python3


import os
import logging
from pathlib import Path
from typing import Optional, List, Union

import pandas as pd
import geopandas as gpd
import shapely.wkt
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union


_LOG = logging.getLogger(__name__)



AUXILIARY_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/auxiliary_new.shp'
BRDF_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/brdf_tiles_new.shp'
AEROSOL_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/aerosol.shp'
ONE_DEG_DSM_V2_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/one-deg-dsm-v2.shp'
OCEAN_MASK_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/auxiliary-extents/ocean_mask.shp'

WRS_SHAPEFILE = '/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/global_wrs_mgrs_shps/wrsdall_Decending.shp'


_SHAPEFILE_LIST = [OCEAN_MASK_SHAPEFILE, AUXILIARY_SHAPEFILE, BRDF_SHAPEFILE, AEROSOL_SHAPEFILE, ONE_DEG_DSM_V2_SHAPEFILE]


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
    """ Code to read a shapefile and return its content as a geopandas dataframe"""
    return gpd.read_file(shapefile)


def _get_auxiliary_extent(gpd_df: Path, subset_key: Optional[str] = None) -> Polygon:
    """ Returns the extent of all auxiliary dataset or an extent by a subset_key"""
    if subset_key:
        return cascaded_union([geom for geom in gpd_df[gpd_df.auxiliary_ == subset_key].geometry])
    return cascaded_union([geom for geom in gpd_df.geometry])


def _get_land_auxiliary_extent(_extents: List[Polygon]) -> Polygon:
    """ Returns the overlaped regions from list of extents derived from auxiliary datasets"""

    overlap_extent = _extents[0]
    for idx, extent in enumerate(_extents, start=1):
        overlap_extent = overlap_extent.intersection(extent)

    return overlap_extent


def path_row_filter(scene_to_filter_list, path_row_list: List[str]) -> None:t
    """Filter scenes to check if path/row of a scene is allowed in a path row list"""
    for path_row in path_row_list:
        print(path_row)


def nbar_scene_filter(
    nbar_auxiliary_extent: Polygon,
    ocean_mask_extent: MultiPolygon,
    df_scenes_to_filter: Union[gpd.GeoDataFrame, Path]
) -> List[str]:
    """ Filtering method to check if acquisition can be used for nbar processing"""

    if isinstance(df_scenes_to_filter, Path):
        df_scenes_to_filter = read_shapefile(df_scenes_to_filter)

    # initial filter is to check if scene intersects with auxiliary data extent
    aux_overlaped_gdf = gpd.GeoDataFrame()
    for idx, geom in enumerate(df_scenes_to_filter.geometry):
        if geom.intersects(nbar_auxiliary_extent):
            aux_overlaped_gdf = aux_overlaped_gdf.append(df_scenes_to_filter.iloc[idx])

    #aux_overlaped_gdf.to_file('/g/data/u46/users/pd1813/Collection_Upgrade/region_code_filter/test_outputs/overlapping_auxiliary_extent.shp', driver="ESRI Shapefile")
    #TODO additional check over ocean mask extent to see if scene is all offshore of inland

    return aux_overlaped_gdf


def main():
    brdf_extent = _get_auxiliary_extent(read_shapefile(BRDF_SHAPEFILE))
    one_deg_dsm_extent = _get_auxiliary_extent(read_shapefile(AUXILIARY_SHAPEFILE), "one_deg_dsm")
    one_sec_dsm_extent = _get_auxiliary_extent(read_shapefile(AUXILIARY_SHAPEFILE), "one_sec_dsm")
    one_deg_dsm_extent_v2 = _get_auxiliary_extent(read_shapefile(ONE_DEG_DSM_V2_SHAPEFILE))

    # use ocean mask extent to identify water atcor processing tiles
    ocean_mask_extent = _get_auxiliary_extent(read_shapefile(OCEAN_MASK_SHAPEFILE))

    # nbar auxiliary extent
    extents = [one_deg_dsm_extent, one_sec_dsm_extent, one_deg_dsm_extent_v2, brdf_extent]
    nbar_extent = _get_land_auxiliary_extent(extents)

    # filter global wrs data
    scenes_df = nbar_scene_filter(nbar_extent, ocean_mask_extent, Path(WRS_SHAPEFILE))

    scenes_to_filter_list = '/g/data/u46/users/pd1813/CollectionUpgrade/landsat_database_scenes.txt'
    # path row filter base on scene names
    path_row_filter(scenes_to_filter_list, scenes_df.PATH_ROW.values)

    '''
    print(brdf_extent)
    print(one_deg_dsm_extent)
    print(one_sec_dsm_extent)
    print(one_deg_dsm_extent_v2)
    '''



if __name__ == '__main__':
    main()

