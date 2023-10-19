#!/usr/bin/env python3

import datetime
from functools import lru_cache
from pathlib import Path

try:
    import tables  # This is needed when testing locally
except ModuleNotFoundError:
    pass
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
        wv_pathname = self.wv_path.joinpath(WV_FMT.format(year=a_year))
        return wv_pathname.exists()

    @lru_cache(maxsize=32)
    def get_wv_index(self, a_year):
        wv_pathname = self.wv_path.joinpath(WV_FMT.format(year=a_year))
        with h5py.File(str(wv_pathname), "r") as fid:
            index = read_h5_table(fid, "INDEX")
        return index

    @lru_cache(maxsize=20000)
    def brdf_day_exists(self, ymd, base_path):
        brdf_day_of_interest = base_path.joinpath(ymd)
        return brdf_day_of_interest.is_dir()

    def check_modis(self, ymd):
        if self.brdf_day_exists(ymd, self.brdf_path):
            return True, ""
        else:
            return False, f"MODIS BRDF data for {ymd} does not exist."

    def check_viirs(self, ymd):
        if self.brdf_day_exists(ymd, self.viirs_i_path) and self.brdf_day_exists(
            ymd, self.viirs_m_path
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
