#!/usr/bin/env python3

import datetime
from pathlib import Path
import structlog
import h5py
import numpy
import pandas

# from wagl.hdf5 import read_h5_table


LOG = structlog.get_logger()
DEFINITIVE_START_DATE = datetime.datetime(2002, 7, 1)
BRDF_DIR = "/g/data/v10/eoancillarydata-2/BRDF/MCD43A1.006"
WV_DIR = "/g/data/v10/eoancillarydata-2/water_vapour"
WV_FMT = "pr_wtr.eatm.{year}.h5"
ACQ_DATES = [
    datetime.datetime(1944, 6, 4),
    datetime.datetime(2001, 12, 31),
    datetime.datetime(2003, 10, 11),
    datetime.datetime.now(),
    datetime.datetime.now() - datetime.timedelta(days=1),
    datetime.datetime.now() - datetime.timedelta(days=2),
    datetime.datetime.now() - datetime.timedelta(days=3),
    datetime.datetime.now() - datetime.timedelta(days=4),
    datetime.datetime.now() - datetime.timedelta(days=5),
    datetime.datetime.now() - datetime.timedelta(days=6),
]

# from wagl
def read_h5_table(fid, dataset_name, dataframe=True):
    """
    Read a HDF5 `TABLE` as a `pandas.DataFrame`.

    :param fid:
        A h5py `Group` or `File` object from which to read the
        dataset from.

    :param dataset_name:
        A `str` containing the pathname of the dataset location.

    :param dataframe:
        A `bool` indicating whether to return as a `pandas.DataFrame`
        or as NumPy structured array. Default is True
        which is to return as a `pandas.DataFrame`.

    :return:
        Either a `pandas.DataFrame` (Default) or a NumPy structured
        array.
    """

    dset = fid[dataset_name]
    idx_names = None

    # grab the index names if we have them
    idx_names = dset.attrs.get("index_names")

    if dataframe:
        if dset.attrs.get("python_type") == "`Pandas.DataFrame`":
            col_names = dset.dtype.names
            dtypes = [dset.attrs["{}_dtype".format(name)] for name in col_names]
            dtype = numpy.dtype(list(zip(col_names, dtypes)))
            data = pandas.DataFrame.from_records(dset[:].astype(dtype), index=idx_names)
        else:
            data = pandas.DataFrame.from_records(dset[:], index=idx_names)
    else:
        data = dset[:]

    return data


def definitive_ancillary_files(acquisition_datetime, brdf_dir=BRDF_DIR, water_vapour_dir=WV_DIR, wv_days_tolerance=1):
    brdf_path = Path(brdf_dir)
    wv_path = Path(water_vapour_dir)

    # results
    wv_metadata = {}

    # get year of acquisition to confirm definitive data
    year = acquisition_datetime.year
    wv_pathname = wv_path.joinpath(WV_FMT.format(year=year))
    if wv_pathname.exists():
        with h5py.File(str(wv_pathname), "r") as fid:
            index = read_h5_table(fid, "INDEX")

        # 1 day tolerance
        max_tolerance = -datetime.timedelta(days=wv_days_tolerance)
        time_delta = index.timestamp - acquisition_datetime
        result = time_delta[(time_delta < datetime.timedelta()) & (time_delta > max_tolerance)]

        if result.shape[0] == 0:
            return False
        else:
            if acquisition_datetime < DEFINITIVE_START_DATE:
                return True
            else:
                ymd = acquisition_datetime.strftime("%Y.%m.%d")
                brdf_day_of_interest = brdf_path.joinpath(ymd)
                if brdf_day_of_interest.exists:
                    return True
                else:
                    return False
    else:
        return False


if __name__ == "__main__":
    for dt in ACQ_DATES:
        filter(dt, BRDF_DIR, WV_DIR)
