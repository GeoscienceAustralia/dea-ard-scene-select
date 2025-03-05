"""
What collections do we have, and where to we expect them?

This is to make tools more foolproof — they should already know where we store our Level 1 data, for example.
"""

from datetime import timedelta, datetime
from pathlib import Path
from typing import List

import yaml
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.model import Range, Dataset
from eodatasets3.utils import default_utc
from structlog.typing import WrappedLogger

from scene_select.check_ancillary import AncillaryFiles
from scene_select.library import ArdProduct, Level1Product, ArdCollection
from scene_select.utils import chopped_scene_id

PACKAGED_DATA = Path(__file__).parent / "data"
AOI_PATH = PACKAGED_DATA / "Australian_AOI.json"


# Constellation
# - excluded days?
# - unique fields?
# - metadata

AOI_FILE = "Australian_AOI.json"
# AOI_FILE = "Australian_AOI_mainland.json"
# AOI_FILE = "Australian_AOI_with_islands.json"


def _make_patterns():
    L9_C2_PATTERN = (
        r"^(?P<sensor>LC)"
        r"(?P<satellite>09)_"
        r"(?P<processingCorrectionLevel>L1TP|L1GT)_"
        r"(?P<wrsPath>[0-9]{3})"
        r"(?P<wrsRow>[0-9]{3})_"
        r"(?P<acquisitionDate>[0-9]{8})_"
        r"(?P<processingDate>[0-9]{8})_"
        r"(?P<collectionNumber>02)_"
        r"(?P<collectionCategory>T1|T2)"
        r"(?P<extension>)$"
    )

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

    S2_PATTERN = r"^(?P<satellite>S2)" + r"(?P<satelliteid>[A-C])_"

    return {
        "ga_ls5t_level1_3": L5_PATTERN,
        "ga_ls7e_level1_3": L7_C1_PATTERN,
        "usgs_ls5t_level1_1": L5_PATTERN,
        "usgs_ls7e_level1_1": L7_C1_PATTERN,
        "usgs_ls7e_level1_2": L7_C2_PATTERN,
        "usgs_ls8c_level1_1": L8_C1_PATTERN,
        "usgs_ls8c_level1_2": L8_C2_PATTERN,
        "usgs_ls9c_level1_2": L9_C2_PATTERN,
        "esa_s2am_level1_0": S2_PATTERN,
        "esa_s2bm_level1_0": S2_PATTERN,
        "esa_s2cm_level1_0": S2_PATTERN,
    }


PROCESSING_PATTERN_MAPPING = _make_patterns()


ANCILLARY_COLLECTION = AncillaryFiles(
    brdf_dir="/g/data/v10/eoancillarydata-2/BRDF/MCD43A1.061",
    wv_dir="/g/data/v10/eoancillarydata-2/water_vapour",
    viirs_i_path="/g/data/v10/eoancillarydata-2/BRDF/VNP43IA1.001",
    viirs_m_path="/g/data/v10/eoancillarydata-2/BRDF/VNP43MA1.001",
    use_viirs_after=datetime(2099, 9, 9),
)

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
    ArdProduct(
        "ga_s2cm_ard_3",
        base_package_directory=Path("/g/data/ka08/ga"),
        sources=[
            Level1Product(
                "esa_s2cm_level1_0",
                base_collection_path=Path("/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C"),
                separate_metadata_directory=Path("/g/data/ka08/ga/l1c_metadata"),
                is_active=True,
            )
        ],
    ),
}


def get_ard_product(product_name: str) -> ArdProduct:
    products = {product for product in ARD_PRODUCTS if product.name == product_name}
    if not products:
        raise ValueError(f"No products found for {product_name=}")
    if len(products) > 1:
        raise RuntimeError(
            f"Multiple products should never be found for one product name? {product_name=}"
        )
    [product] = products
    return product


def get_ard_for_level1(level1_product_name: str) -> ArdProduct:
    def _source_names(product):
        return (p.name for p in product.sources)

    # Find the product that has this level1 as a source.
    products = {
        product
        for product in ARD_PRODUCTS
        if level1_product_name in _source_names(product)
    }
    if not products:
        raise ValueError(f"No products found with source {level1_product_name=}")
    if len(products) > 1:
        raise RuntimeError(
            f"Multiple products should never be found for one product name? {level1_product_name=}"
        )
    [product] = products
    return product


def get_collection(dc: Datacube, prefix: str = None) -> ArdCollection:
    products = {
        product for product in ARD_PRODUCTS if product.name.startswith(f"ga_{prefix}")
    }

    if not products:
        raise ValueError(f"No products found for {prefix=}")

    return ArdCollection(dc=dc, ard_products=products, aoi_path=AOI_PATH)


def index_level1_path(metadata_path: Path, d_log: WrappedLogger) -> bool:
    """
    Add a dataset to the currently-configured datacube.
    """

    with metadata_path.open("r") as f:
        doc = yaml.safe_load(f)

    with Datacube(app="usgs-l1-dl") as dc:
        #: Tuple[Dataset, str]
        (dataset, error_message) = Doc2Dataset(dc.index)(doc, metadata_path.as_uri())
        if dataset is None:
            d_log.error("dataset_load_failure", error_msg=error_message)
            return False

        d_log = d_log.bind(
            dataset_id=dataset.id,
            landsat_scene_id=dataset.metadata.landsat_scene_id,
        )
        # Are there existing, earlier datasets of this USGS scene?
        scene_id = chopped_scene_id(dataset.metadata.landsat_scene_id)
        previous_dataset_versions: List[Dataset] = dc.index.datasets.search_eager(
            product_family="level1",
            platform=dataset.metadata.platform,
            region_code=dataset.metadata.region_code,
            time=Range(
                dataset.time.begin - timedelta(days=1),
                dataset.time.end + timedelta(days=1),
            ),
        )
        for previous_dataset in previous_dataset_versions:
            previous_scene_id = previous_dataset.metadata.landsat_scene_id
            if previous_dataset.id == dataset.id:
                d_log.warn("already_exists")
                return False

            # Skip things of another scene (probably different platform?
            if not previous_dataset.metadata.landsat_scene_id.startswith(scene_id):
                # The scene is not of the same landsat_scene_id.
                # This shouldn't happen unless some new similar products are added to
                # datacube in the future...
                d_log.warn(
                    "mismatched_scene_result",
                    matched_on=previous_dataset.id,
                    matched_on_scene=previous_scene_id,
                )
                return False

            # Was it processed after our new one? Then stop.
            if default_utc(previous_dataset.metadata.creation_time) > default_utc(
                dataset.metadata.creation_time
            ):
                d_log.warn(
                    "skip.newer_dataset_exists",
                    previous_dataset_id=previous_dataset.id,
                    previous_dataset_time=previous_dataset.metadata.creation_time,
                )
                return False

            d_log.info(
                "archiving_previous_dataset",
                previous_dataset_id=previous_dataset.id,
                previous_scene_id=previous_scene_id,
            )
            dc.index.datasets.archive([previous_dataset.id])

        d_log.info("do_indexing")
        dc.index.datasets.add(dataset)
        return True
