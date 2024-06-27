"""
What collections do we have, and where to we expect them?

This is to make tools more foolproof — they should already know where we store our Level 1 data, for example.
"""

import calendar
import datetime
import logging

from pathlib import Path
from typing import Optional, List, Generator, Tuple

from attr import define, field
from datacube import Datacube
from datacube.model import Range, Dataset
from datacube.utils import uri_to_local_path
from ruamel import yaml

_LOG = logging.getLogger(__name__)


@define
class Level1Product:
    name: str

    # Examples:
    # /g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2021/2021-01/30S110E-35S115E/S2B_MSIL1C_20210124T023249_N0209_R103_T50JLL_20210124T035242.zip
    # /g/data/da82/AODH/USGS/L1/Landsat/C2/092_079/LC80920792024074/LC08_L1TP_092079_20240314_20240401_02_T1.odc-metadata.yaml

    base_collection_path: Path

    # The metadata, if it's stored separately from the L1 data itself.
    #    (if None, assuming metadata sits alongise the data)
    separate_metadata_directory: Optional[Path] = None

    # Is this still receiving new data? ie. do we expect ongoing downloads
    #    (false if the satellite is retired, or if a newer collection is available)
    is_active: bool = False


@define(unsafe_hash=True)
class ArdProduct:
    name: str = field(eq=True, hash=True)
    base_package_directory: Path = field(eq=False, hash=False)

    # Source level1s
    sources: List[Level1Product] = field(eq=False, hash=False)

    # Example:
    # /g/data/xu18/ga/ga_ls8c_ard_3/089/074/2024/05/27/ga_ls8c_ard_3-2-1_089074_2024-05-27_final.odc-metadata.yaml


PACKAGED_DATA = Path(__file__).parent / "data"


@define
class Aoi:
    path: Path


@define
class BaseDataset:
    dataset_id: str = field(eq=True, hash=True)
    metadata_path: Path = field(eq=False, hash=False)

    # TODO: cached_property?
    def metadata_doc(self) -> dict:
        with self.metadata_path.open() as f:
            return yaml.safe_load(f)


@define(unsafe_hash=True)
class Level1Dataset(BaseDataset):
    # The zip or tar file
    data_path: Path = field(eq=False, hash=False)

    @classmethod
    def from_odc(cls, dataset: Dataset, product: Level1Product):
        """
        Create a Level1Dataset from a datacube.Dataset
        """

        # TODO: For now, assuming one uri
        uri = dataset.uris[0]

        if product.name.startswith("esa_s2"):
            # S2 is indexed as zip:// URIs
            # Eg. 'zip:/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2019/2019-06/10S125E-15S130E/S2A_MSIL1C_20190607T014701_N0207_R074_T51LZD_20190607T031912.zip!/'

            data_path = zip_uri_to_path(uri)
            # And metadata is found in `product.separate_metadata_directory`
            # It is under the same subfolder structure of that directory, as the data_path is under `product.base_collection_path`
            metadata_path = product.separate_metadata_directory / data_path.relative_to(
                product.base_collection_path
            ).with_suffix(".odc-metadata.yaml")

            if not metadata_path.exists():
                all_granule_metadatas = list(metadata_path.parent.glob(f'{data_path.stem}*.odc-metadata.yaml'))
                # All file have an `id` field, so we can find which one matches dataset.id
                for granule_metadata in all_granule_metadatas:
                    with granule_metadata.open() as f:
                        granule_doc = yaml.safe_load(f)
                        if str(granule_doc['id']) == str(dataset.id):
                            metadata_path = granule_metadata
                            break
                        _LOG.debug(f"Filtering granule with id {granule_doc['id']}!={dataset.id} for {granule_metadata}")
                else:
                    raise ValueError(
                        f"Could not find metadata for {data_path}, tried {metadata_path} and {all_granule_metadatas}"
                    )
        elif product.name.startswith("usgs_ls") or product.name.startswith("ga_ls"):
            # Landsat is indexed as file:// URIs (sadly, without correct tar URI for data access)
            # Eg. 'file:///g/data/da82/AODH/USGS/L1/Landsat/C2/092_079/LC80920792024074/LC08_L1TP_092079_20240314_20240401_02_T1.odc-metadata.yaml'
            #     For data: /g/data/da82/AODH/USGS/L1/Landsat/C2/092_079/LC80920792024074/LC08_L1TP_092079_20240314_20240401_02_T1/LC08_L1TP_092079_20240314_20240401_02_T1.tar
            metadata_path = uri_to_local_path(uri)
            data_path = metadata_path.with_name(
                metadata_path.name.replace(".odc-metadata.yaml", ".tar")
            )
            if not data_path.exists():
                raise ValueError(
                    f"Could not find tar file for {metadata_path}, tried {data_path}"
                )
        else:
            raise ValueError(f"Unknown product type {product.name}")

        return Level1Dataset(
            dataset_id=str(dataset.id),
            metadata_path=metadata_path,
            data_path=data_path,
        )


def zip_uri_to_path(uri: str) -> Path:
    """
    >>> str(zip_uri_to_path('zip:/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2019/2019-06/10S125E-15S130E/S2A_MSIL1C_20190607T014701_N0207_R074_T51LZD_20190607T031912.zip!/'))
    '/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2019/2019-06/10S125E-15S130E/S2A_MSIL1C_20190607T014701_N0207_R074_T51LZD_20190607T031912.zip'
    """
    prefix = "zip:"
    if not uri.startswith(prefix):
        raise ValueError(f"Expected {uri=} to start with {prefix=}")
    return Path(uri.split("!")[0][len(prefix):])


@define
class ArdDataset(BaseDataset):
    maturity: str = field(eq=False, hash=False)

    @property
    def proc_info_path(self) -> Path:
        accessories = self.metadata_doc()["accessories"]
        if "metadata:processor" not in accessories:
            raise ValueError(f"No processor metadata found in {self.metadata_path}")

        # TODO: This should properly handle different subfolders/etc
        return self.metadata_path.with_name(accessories["metadata:processor"]["path"])

    # TODO: cached_property?
    def proc_info_doc(self) -> dict:
        with self.proc_info_path.open() as f:
            return yaml.safe_load(f)

    def software_versions(self):
        """
        Extract the list of versions, and return as a flat dict of name->version
        """
        return {
            item["name"]: item["version"]
            for item in self.proc_info_doc()["software_versions"]
        }

    @property
    def level1_id(self):
        return self.metadata_doc()["lineage"]["level1"][0]


def month_as_range(year: int, month: int) -> Range:
    """
    >>> month_as_range(2024, 2)
    Range(begin=datetime.datetime(2024, 2, 1, 0, 0), end=datetime.datetime(2024, 2, 29, 23, 59, 59, 999999))
    >>> month_as_range(2023, 12)
    Range(begin=datetime.datetime(2023, 12, 1, 0, 0), end=datetime.datetime(2023, 12, 31, 23, 59, 59, 999999))
    """
    week_day, number_of_days = calendar.monthrange(year, month)
    return Range(
        datetime.datetime(year, month, 1),
        datetime.datetime(year, month, number_of_days, 23, 59, 59, 999999),
    )


class ArdCollection:
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
                    base_collection_path=Path(
                        "/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C"
                    ),
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
                    base_collection_path=Path(
                        "/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C"
                    ),
                    separate_metadata_directory=Path("/g/data/ka08/ga/l1c_metadata"),
                    is_active=True,
                )
            ],
        ),
    }

    def __init__(
        self,
        dc: Datacube,
        prefix: str = "",
        aoi_path=PACKAGED_DATA / "Australian_AOI.json",
    ):
        """
        Prefix will determine the set of products you want

        All product names that match `ga_{prefix}*`

        Examples:
        - "ls" for all landsat products
        - "ls9" for landsat 9 products.
        - "s2" for all s2 products

        """
        self.dc = dc
        self.aoi: Aoi = Aoi(path=aoi_path)

        self.products = {
            product
            for product in self.ARD_PRODUCTS
            if product.name.startswith(f"ga_{prefix}")
        }

        if not self.products:
            raise ValueError(f"No products found for {prefix=}")

    # def iterate_processable_levels1s(self): ...
    def iterate_indexed_ard_datasets(
        self,
    ) -> Generator[Tuple[ArdProduct, ArdDataset], None, None]:
        for product in self.products:
            product_start_time, product_end_time = (
                self.dc.index.datasets.get_product_time_bounds(product=product.name)
            )
            # Query month-by-month to make DB queries smaller.

            seen_dataset_ids = set()
            for year in range(product_start_time.year, product_end_time.year + 1):
                for month in range(1, 13):
                    _LOG.debug("Searching %s %s-%s", product.name, year, month)
                    for (
                        dataset_id,
                        maturity,
                        uri,
                    ) in self.dc.index.datasets.search_returning(
                        ("id", "dataset_maturity", "uri"),
                        product=product.name,
                        time=month_as_range(year, month),
                    ):
                        # Note that we may receive the same dataset multiple times due to time boundaries
                        # (hence: record our seen ones)
                        if dataset_id not in seen_dataset_ids:
                            yield (
                                product,
                                ArdDataset(
                                    dataset_id=dataset_id,
                                    maturity=maturity,
                                    metadata_path=uri_to_local_path(uri),
                                ),
                            )
                            seen_dataset_ids.add(dataset_id)
