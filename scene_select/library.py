import calendar
import datetime

from pathlib import Path
from typing import Optional, List, Generator, Tuple, Iterable

import structlog
from attr import define, field
from datacube import Datacube
from datacube.model import Range, Dataset
from datacube.utils import uri_to_local_path
from eodatasets3.utils import default_utc
from ruamel import yaml


_LOG = structlog.get_logger()


@define
class BaseProduct:
    name: str = field(eq=True, hash=True)

    @property
    def constellation_code(self) -> str:
        """
        Is this an 'ls' or 's2' product?
        """
        return _get_constellation_code_for_product(self.name)


@define(hash=True)
class Level1Product(BaseProduct):
    # Examples:
    # /g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2021/2021-01/30S110E-35S115E/S2B_MSIL1C_20210124T023249_N0209_R103_T50JLL_20210124T035242.zip
    # /g/data/da82/AODH/USGS/L1/Landsat/C2/092_079/LC80920792024074/LC08_L1TP_092079_20240314_20240401_02_T1.odc-metadata.yaml

    base_collection_path: Path = field(eq=False, hash=False)

    # The metadata, if it's stored separately from the L1 data itself.
    #    (if None, assuming metadata sits alongise the data)
    separate_metadata_directory: Optional[Path] = field(
        eq=False, hash=False, default=None
    )

    # Is this still receiving new data? ie. do we expect ongoing downloads
    #    (false if the satellite is retired, or if a newer collection is available)
    is_active: bool = field(eq=False, hash=False, default=False)


@define(unsafe_hash=True)
class ArdProduct(BaseProduct):
    base_package_directory: Path = field(eq=False, hash=False)

    # Source level1s
    sources: List[Level1Product] = field(eq=False, hash=False)

    # Example:
    # /g/data/xu18/ga/ga_ls8c_ard_3/089/074/2024/05/27/ga_ls8c_ard_3-2-1_089074_2024-05-27_final.odc-metadata.yaml


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
    product: Level1Product = field(eq=False, hash=False)
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
                all_granule_metadatas = list(
                    metadata_path.parent.glob(f"{data_path.stem}*.odc-metadata.yaml")
                )
                # All file have an `id` field, so we can find which one matches dataset.id
                for granule_metadata in all_granule_metadatas:
                    with granule_metadata.open() as f:
                        granule_doc = yaml.safe_load(f)
                        if str(granule_doc["id"]) == str(dataset.id):
                            metadata_path = granule_metadata
                            break
                        # _LOG.debug(
                        #     "filtered_different_id",
                        #     document_dataset_id=granule_doc["id"],
                        #     our_dataset_id=dataset.id,
                        #     metadata_path=granule_metadata,
                        # )
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
            product=product,
        )


def zip_uri_to_path(uri: str) -> Path:
    """
    >>> str(zip_uri_to_path('zip:/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2019/2019-06/10S125E-15S130E/S2A_MSIL1C_20190607T014701_N0207_R074_T51LZD_20190607T031912.zip!/'))
    '/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2019/2019-06/10S125E-15S130E/S2A_MSIL1C_20190607T014701_N0207_R074_T51LZD_20190607T031912.zip'
    """
    prefix = "zip:"
    if not uri.startswith(prefix):
        raise ValueError(f"Expected {uri=} to start with {prefix=}")
    return Path(uri.split("!")[0][len(prefix) :])


def _get_constellation_code_for_product(product_name: str) -> str:
    """
    >>> _get_constellation_code_for_product('ga_ls5t_level1_3')
    'ls'
    >>> _get_constellation_code_for_product('usgs_ls9c_level1_2')
    'ls'
    >>> _get_constellation_code_for_product('ga_ls8c_ard_3')
    'ls'
    >>> _get_constellation_code_for_product('esa_s2bm_level1_0')
    's2'
    >>> _get_constellation_code_for_product('esa_s2cm_level1_0')
    's2'
    >>> _get_constellation_code_for_product('ga_s2cm_ard_3')
    's2'
    >>> # probably don't need to handle this...
    >>> _get_constellation_code_for_product('ga_ls_landcover_class_cyear_3')
    'ls'
    """
    # Let's be ultra safe by complaining loudly if something unusual pops up.
    prefix = product_name.split("_")[1][:2]
    if prefix not in ("ls", "s2"):
        raise ValueError(
            f"Unknown constellation {prefix} for {product_name=}! Something is wrong"
        )
    return prefix


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

    @classmethod
    def from_odc(cls, ard_dataset: Dataset):
        return ArdDataset(
            dataset_id=str(ard_dataset.id),
            metadata_path=ard_dataset.local_path,
            maturity=ard_dataset.metadata.dataset_maturity,
        )


class ArdCollection:
    def __init__(
        self,
        dc: Datacube,
        ard_products: Iterable[ArdProduct],
        aoi_path: Path,
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

        self.ard_products = list(ard_products)

    @property
    def constellation_code(self) -> str:
        """
        Is this an 'ls' or 's2' collection?
        """
        # Sanity check. Every collection will only have one constellation!
        constellations = set(p.constellation_code for p in self.ard_products)
        if len(constellations) != 1:
            raise ValueError(
                f"All products must be from the same constellation, got {constellations} for {self.ard_products}"
            )

        [constellation] = constellations
        return constellation

    def iter_level1_products(self) -> Generator[Level1Product, None, None]:
        for ard_product in self.ard_products:
            yield from ard_product.sources

    # def iterate_processable_levels1s(self): ...
    def iterate_indexed_ard_datasets(
        self,
        search_expressions: dict,
    ) -> Generator[Tuple[ArdProduct, ArdDataset], None, None]:
        for product in self.ard_products:
            if (
                "product" in search_expressions
                and search_expressions["product"] != product.name
            ):
                continue

            expressions = search_expressions.copy()

            if "time" in search_expressions:
                product_start_time, product_end_time = expressions.pop("time")
            else:
                _LOG.info("finding_product_time_bounds", product_name=product.name)
                (
                    product_start_time,
                    product_end_time,
                ) = self.dc.index.datasets.get_product_time_bounds(product=product.name)

            seen_dataset_ids = set()

            # Query month-by-month to make DB queries smaller.
            for time in iterate_as_months(product_start_time, product_end_time):
                _LOG.info(
                    "searching_time_block",
                    product_name=product.name,
                    time=displayable_date_range(time),
                )

                for (
                    dataset_id,
                    maturity,
                    uri,
                ) in self.dc.index.datasets.search_returning(
                    ("id", "dataset_maturity", "uri"),
                    product=product.name,
                    time=time,
                    **expressions,
                ):
                    # Note that we may receive the same dataset multiple times due to time boundaries
                    # (hence: record our seen ones)
                    if dataset_id not in seen_dataset_ids:
                        yield (
                            product,
                            ArdDataset(
                                dataset_id=str(dataset_id),
                                maturity=maturity,
                                metadata_path=uri_to_local_path(uri),
                            ),
                        )
                        seen_dataset_ids.add(dataset_id)


def month_as_range(
    year: int, month: int, start_time: datetime.datetime, end_time: datetime.datetime
) -> Range:
    """
    Returns a Range object for the given year and month, strictly constrained by start_time and end_time.

    >>> start = datetime.datetime(2024, 2, 15, 10, 30)
    >>> end = datetime.datetime(2024, 3, 10, 14, 45)
    >>> month_as_range(2024, 2, start, end)
    Range(begin=datetime.datetime(2024, 2, 15, 10, 30), end=datetime.datetime(2024, 2, 29, 23, 59, 59, 999999))
    >>> month_as_range(2024, 3, start, end)
    Range(begin=datetime.datetime(2024, 3, 1, 0, 0), end=datetime.datetime(2024, 3, 10, 14, 45))
    """
    month_start = datetime.datetime(year, month, 1, tzinfo=start_time.tzinfo)
    _, last_day = calendar.monthrange(year, month)
    month_end = datetime.datetime(
        year, month, last_day, 23, 59, 59, 999999, tzinfo=start_time.tzinfo
    )

    range_start = max(month_start, start_time)
    range_end = min(month_end, end_time)

    return Range(range_start, range_end)


def iterate_as_months(start_time: datetime.datetime, end_time: datetime.datetime):
    """
    Iterate the time range as individual months (or smaller).
    """
    start_time = default_utc(start_time)
    end_time = default_utc(end_time)

    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_time:
        year, month = current.year, current.month
        yield month_as_range(year, month, start_time, end_time)

        # Move to the next month
        if month == 12:
            current = current.replace(year=year + 1, month=1)
        else:
            current = current.replace(month=month + 1)


def displayable_date_range(range: Range):
    begin: datetime.datetime = range.begin
    end: datetime.datetime = range.end
    return f"({begin.isoformat()}, {end.isoformat()})"
