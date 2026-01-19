#!/usr/bin/env python3

import datetime
from functools import lru_cache
from pathlib import Path

try:
    import tables  # This is needed when testing locally
except ModuleNotFoundError:
    pass
import os
import h5py
import numpy
import pandas
import structlog

LOG = structlog.get_logger()
MODIS_START_DATE = datetime.datetime(2002, 7, 1)
DEFAULT_MODIS_DIR = "/g/data/v10/eoancillarydata-2/BRDF/MCD43A1.061"
DEFAULT_VIIRS_I_PATH = "/g/data/v10/eoancillarydata-2/BRDF/VNP43IA1.001"  # viirs_i_path
DEFAULT_VIIRS_M_PATH = "/g/data/v10/eoancillarydata-2/BRDF/VNP43MA1.001"  # viirs_m_path
DEFAULT_USE_VIIRS_AFTER = datetime.datetime(2099, 9, 9)
WV_DIR = "/g/data/v10/eoancillarydata-2/water_vapour"
WV_FMT = "pr_wtr.eatm.{year}.h5"


USE_S3 = os.environ.get("USE_S3", "true").lower() == "true"
S3_BUCKET = os.environ.get("S3_BUCKET", "ard-processing-data")
# S3_BRDF_PREFIX = os.environ.get("S3_PREFIX", "ancillary")
S3_PREFIX = os.environ.get("S3_PREFIX", "ancillary")
WATER_VAPOUR_PREFIX = "water_vapour"
BRDF_PREFIX = "BRDF"
if USE_S3:
    import boto3
    import logging

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").propagate = True
    logging.getLogger("s3transfer").propagate = True
    logging.getLogger("urllib3").propagate = True

    S3_CLIENT = boto3.client("s3")


def to_s3_key(base_path: Path, path: Path) -> str:
    return str(Path(S3_PREFIX) / path.relative_to(base_path))


def to_file_path(base_path: Path, s3_key: str) -> Path:
    return base_path / s3_key.relative_to(S3_PREFIX)


def dir_exists(base_path: Path, path: Path) -> bool:
    """List children of the given path, either locally or in S3, depending on configuration."""
    print(f"GETTING CHILDREN FOR {base_path} {path}")
    if USE_S3:
        s3_path = to_s3_key(base_path, path)
        print(f"LISTING S3 PATH AT {s3_path}")
        response = S3_CLIENT.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_path)
        ans= response.get("KeyCount", 0) > 0
        print("RETURNING", ans)
        return ans
    else:
        return path.is_dir()


def download_file(base_path: Path, path: Path) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    s3_key = to_s3_key(base_path, path)
    print("DOWNLOADING", s3_key, "to", path)
    S3_CLIENT.download_file(S3_BUCKET, s3_key, path)


def file_exists(base_path: Path, path: Path) -> bool:
    """Check if the file exists either locally or in S3, depending on configuration."""
    from botocore.exceptions import ClientError

    if USE_S3:
        try:
            s3_key = to_s3_key(base_path, path)
            print("LOOKING FOR KEY", s3_key)
            S3_CLIENT.head_object(Bucket=S3_BUCKET, Key=s3_key)
            return True
        except ClientError as e:
            # Object does not exist.
            if e.response["Error"]["Code"] == "404":
                return False
            # For any other error (e.g., 403 Forbidden, 500 Server Error), re-raise the exception
            else:
                return path.exists()
    else:
        return False


def read_h5_table(fid, dataset_name):
    """
    From Wagl. Read a HDF5 `TABLE` as a `pandas.DataFrame`.

    :param fid:
        A h5py `Group` or `File` object from which to read the
        dataset from.

    :param dataset_name:
        A `str` containing the pathname of the dataset location.

    :return:
        Either a `pandas.DataFrame` (Default) or a NumPy structured
        array.
    """

    dset = fid[dataset_name]

    # grab the index names if we have them
    idx = dset.attrs.get("index_names")

    if dset.attrs.get("python_type") == "`Pandas.DataFrame`":
        col_names = dset.dtype.names
        dtypes = [dset.attrs[f"{name}_dtype"] for name in col_names]
        dtype = numpy.dtype(list(zip(col_names, dtypes)))
        data = pandas.DataFrame.from_records(dset[:].astype(dtype), index=idx)
    else:
        data = pandas.DataFrame.from_records(dset[:], index=idx)
    return data


class AncillaryFiles:
    def __init__(
        self,
        brdf_dir=DEFAULT_MODIS_DIR,
        wv_dir=WV_DIR,
        viirs_i_path=DEFAULT_VIIRS_I_PATH,
        viirs_m_path=DEFAULT_VIIRS_M_PATH,
        use_viirs_after=DEFAULT_USE_VIIRS_AFTER,
        wv_days_tolerance=1,
    ):
        self.brdf_path = Path(brdf_dir)
        self.wv_path = Path(wv_dir)  # water_vapour_dir
        self.viirs_i_path = Path(viirs_i_path)
        self.viirs_m_path = Path(viirs_m_path)
        self.use_viirs_after = use_viirs_after
        self.max_tolerance = -datetime.timedelta(days=wv_days_tolerance)

    @lru_cache(maxsize=32)
    def wv_file_exists(self, a_year):
        wv_pathname = self.wv_path.joinpath(WATER_VAPOUR_PREFIX).joinpath(
            WV_FMT.format(year=a_year)
        )
        print("WE ARE LOOKING AT ", wv_pathname)
        return file_exists(self.wv_path, wv_pathname)
        return wv_pathname.exists()

    @lru_cache(maxsize=32)
    def get_wv_index(self, a_year):
        wv_pathname = self.wv_path.joinpath(WATER_VAPOUR_PREFIX).joinpath(
            WV_FMT.format(year=a_year)
        )
        print("WE READING THE INDEX AT ", wv_pathname)
        download_file(self.wv_path, wv_pathname)
        with h5py.File(str(wv_pathname), "r") as fid:
            index = read_h5_table(fid, "INDEX")
        return index

    @lru_cache(maxsize=20000)
    def brdf_day_exists(self, ymd, base_path):
        brdf_day_of_interest = base_path.joinpath(ymd)
        print("WE ARE LOOKING FOR FOLDER ", brdf_day_of_interest)
        return dir_exists(base_path, brdf_day_of_interest)

    def check_modis(self, ymd):
        if self.brdf_day_exists( "BRDF/MCD43A1.061/" + ymd, self.brdf_path):
            return True, ""
        else:
            return False, f"MODIS BRDF data for {ymd} does not exist."

    def check_viirs(self, ymd):
        if self.brdf_day_exists("BRDF/VNP43IA1.002/" + ymd, self.viirs_i_path) and self.brdf_day_exists(
            "BRDF/VNP43MA1.002/" + ymd, self.viirs_m_path
        ):
            return True, ""
        else:
            return False, f"VIIRS BRDF data for {ymd} does not exist."

    def ancillary_files(self, acquisition_datetime):
        if not self.wv_file_exists(acquisition_datetime.year):
            return (
                False,
                "No water vapour data for year {}.".format(acquisition_datetime.year),
            )

        # get year of acquisition to confirm definitive data
        index = self.get_wv_index(acquisition_datetime.year)

        # Removing timezone info since different UTC formats were clashing.
        acquisition_datetime = acquisition_datetime.replace(tzinfo=None)

        delta = index.timestamp - acquisition_datetime
        afilter = (delta < datetime.timedelta()) & (delta > self.max_tolerance)
        result = delta[afilter]

        if result.shape[0] == 0:
            return (
                False,
                "Water vapour data for {} does not exist.".format(acquisition_datetime),
            )
        else:
            ymd = acquisition_datetime.strftime("%Y.%m.%d")
            if acquisition_datetime < MODIS_START_DATE:
                return True, ""
            elif acquisition_datetime < self.use_viirs_after:
                return self.check_modis(ymd)
            else:
                # use viirs
                return self.check_viirs(ymd)


if __name__ == "__main__":
    pass
