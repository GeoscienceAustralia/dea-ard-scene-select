#from region_code_filter import _do_search
import datacube
from datetime import datetime, timedelta
import logging

_LOG = logging.getLogger(__name__)


def _do_search(dc, expressions, days_delta=0, only_childless=True):
    for dataset in dc.index.datasets.search(**expressions):
        metadata_path = dataset.local_path
        if not dataset.local_path:
            _LOG.warning("Skipping dataset without local paths: %s", dataset.id)
            continue
        assert metadata_path.name.endswith("metadata.yaml")
        weeks_ago_3 = datetime.now(dataset.time.end.tzinfo) - timedelta(days=days_delta)
        if weeks_ago_3 < dataset.time.end:
            _LOG.warning("Skipping dataset after time delta(days:%d, Date %s): %s", days_delta,
            weeks_ago_3.strftime('%Y-%m-%d'), dataset.id)
            continue
        # Name of input folder treated as telemetry dataset name
        name = metadata_path.parent.name

        if only_childless:
            # If any child exists that isn't archived
            if any(
                not child_dataset.is_archived
                for child_dataset in dc.index.datasets.get_derived(dataset.id)
            ):
                _LOG.debug(
                    "Skipping dataset with children: %s (%s)", name, dataset.id
                )
                continue
        file_path = dataset.local_path.parent.joinpath(dataset.metadata.landsat_product_id).with_suffix(".tar").as_posix()

        yield dataset

past = datetime.now() - timedelta(days=21)

dc = datacube.Datacube(app="gen-list")

product = "ga_ls5t_level1_3"
gen = _do_search(dc, {'product':product}, days_delta=12410)
results = next(gen)
print (results)
#print (help(results))