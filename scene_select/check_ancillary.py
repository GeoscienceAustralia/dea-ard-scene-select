#!/usr/bin/env python3

import datetime
from functools import lru_cache
from pathlib import Path

# This is needed when testing locally
# import hdf5plugin
import h5py
import numpy
import pandas
import structlog

LOG = structlog.get_logger()
BRDF_DEFINITIVE_START_DATE = datetime.datetime(2002, 7, 1)
BRDF_DIR = "/g/data/v10/eoancillarydata-2/BRDF/MCD43A1.006"
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
    def __init__(self, brdf_dir=BRDF_DIR, wv_dir=WV_DIR, wv_days_tolerance=1):
        self.brdf_path = Path(brdf_dir)
        self.wv_path = Path(wv_dir)  # water_vapour_dir
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
    def brdf_day_exists(self, ymd):
        brdf_day_of_interest = self.brdf_path.joinpath(ymd)
        return brdf_day_of_interest.exists()

    def ancillary_files(self, acquisition_datetime):

        if not self.wv_file_exists(acquisition_datetime.year):
            return False, "No ater vapour data for year {}.".format(
                acquisition_datetime.year
            )

        # get year of acquisition to confirm definitive data
        index = self.get_wv_index(acquisition_datetime.year)

        # Removing timezone info since different UTC formats were clashing.
        acquisition_datetime = acquisition_datetime.replace(tzinfo=None)

        delta = index.timestamp - acquisition_datetime
        afilter = (delta < datetime.timedelta()) & (delta > self.max_tolerance)
        result = delta[afilter]

        if result.shape[0] == 0:
            return False, "Water vapour data for {} does not exist.".format(
                acquisition_datetime
            )
        else:
            if acquisition_datetime < BRDF_DEFINITIVE_START_DATE:
                return True, ""
            else:
                ymd = acquisition_datetime.strftime("%Y.%m.%d")
                if self.brdf_day_exists(ymd):
                    return True, ""
                else:
                    return False, f"BRDF data for {ymd} does not exist."


if __name__ == "__main__":
    pass
