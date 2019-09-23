#!/usr/bin/env python3


import os
import logging
from pathlib import Path
from typing import Optional, List

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


def nbar_scene_filter(nbar_auxiliary_extent: Polygon, ocean_mask_extent: MultiPolygon) -> List[str]:
    """ Filtering method to check if acquisition can be used for nbar processing"""
    #TODO generate list of path/row and mgrs tiles that meets conditions:
    # 1) scene needs to overlap with nbar_auxiliary_extent
    # 2) scene needs overlap with ocean_mask_extent





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




    print(brdf_extent)
    print(one_deg_dsm_extent)
    print(one_sec_dsm_extent)
    print(one_deg_dsm_extent_v2)




if __name__ == '__main__':
    main()

