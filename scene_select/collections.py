"""
What collections do we have, and where to we expect them?

This is to make tools more foolproof — they should already know where we store our Level 1 data, for example.
"""

from pathlib import Path
from datacube import Datacube
from scene_select.library import ArdProduct, Level1Product, ArdCollection

PACKAGED_DATA = Path(__file__).parent / "data"
AOI_PATH = PACKAGED_DATA / "Australian_AOI.json"

ARD_PRODUCTS = {
    ArdProduct(
        "ga_ls5t_ard_3",
        base_package_directory=Path("/g/data/xu18/ga"),
        sources=[
            Level1Product(
                "usgs_ls5t_level1_1",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C1"),
            ),
            Level1Product(
                "ga_ls5t_level1_3",
                base_collection_path=Path("/g/data/da82/AODH/GA/L1/Landsat/C1"),
            ),
        ],
    ),
    ArdProduct(
        "ga_ls7e_ard_3",
        base_package_directory=Path("/g/data/xu18/ga"),
        sources=[
            Level1Product(
                "usgs_ls7e_level1_2",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C2"),
            ),
            Level1Product(
                "usgs_ls7e_level1_1",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C1"),
            ),
            Level1Product(
                "ga_ls7e_level1_3",
                base_collection_path=Path("/g/data/da82/AODH/GA/L1/Landsat/C1"),
            ),
        ],
    ),
    ArdProduct(
        "ga_ls8c_ard_3",
        base_package_directory=Path("/g/data/xu18/ga"),
        sources=[
            Level1Product(
                "usgs_ls8c_level1_2",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C2"),
                is_active=True,
            ),
            Level1Product(
                "usgs_ls8c_level1_1",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C1"),
            ),
        ],
    ),
    ArdProduct(
        "ga_ls9c_ard_3",
        base_package_directory=Path("/g/data/xu18/ga"),
        sources=[
            Level1Product(
                "usgs_ls9c_level1_2",
                base_collection_path=Path("/g/data/da82/AODH/USGS/L1/Landsat/C2"),
                is_active=True,
            )
        ],
    ),
    ArdProduct(
        "ga_s2am_ard_3",
        base_package_directory=Path("/g/data/ka08/ga"),
        sources=[
            Level1Product(
                "esa_s2am_level1_0",
                base_collection_path=Path("/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C"),
                separate_metadata_directory=Path("/g/data/ka08/ga/l1c_metadata"),
                is_active=True,
            )
        ],
    ),
    ArdProduct(
        "ga_s2bm_ard_3",
        base_package_directory=Path("/g/data/ka08/ga"),
        sources=[
            Level1Product(
                "esa_s2bm_level1_0",
                base_collection_path=Path("/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C"),
                separate_metadata_directory=Path("/g/data/ka08/ga/l1c_metadata"),
                is_active=True,
            )
        ],
    ),
}


def get_collection(dc: Datacube, prefix: str) -> ArdCollection:
    products = {
        product for product in ARD_PRODUCTS if product.name.startswith(f"ga_{prefix}")
    }
    if not products:
        raise ValueError(f"No products found for {prefix=}")

    return ArdCollection(dc=dc, products=products, aoi_path=AOI_PATH)
