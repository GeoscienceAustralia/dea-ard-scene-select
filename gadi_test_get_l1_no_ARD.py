
import datacube
import logging
import yaml


_LOG = logging.getLogger(__name__)


def _do_search(expressions, only_childless=True):
    dc = datacube.Datacube()
    for dataset in dc.index.datasets.search(**expressions):

        metadata_path = dataset.local_path
        if not dataset.local_path:
            _LOG.warning("Skipping dataset without local paths: %s", dataset.id)
            continue
        print (metadata_path.name)
        # metadata_path.name = LC08_L1TP_100079_20190120_20190201_01_T1.odc-metadata.yaml
        assert metadata_path.name.endswith("metadata.yaml")
        md = yaml.safe_load(metadata_path.open())
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
        # Expected_out_directory = Path(output_base).joinpath(
        #     output_pattern.format(
        #         time=(md["acquisition"]["aos"]),
        #         sat_number=(md["platform"]["code"].split("_")[-1]),
        #         name=name,
        #         id=md["id"],
        #     )
        # )

        yield metadata_path.parent, metadata_path, file_path



# dc.index.datasets.search(limit=limit, product=self.config.specification.product, **query)
# datasets = dc.index.datasets.search(product=product)

# print(datasets)
# tasks = [ds for ds in datasets]
# print (tasks[0]) # That's ok.  It is returning a yaml though or a dataste that is represented by the yaml
# print (tasks[0])
# print (tasks[0].get_locations())

#metadata_path_parent, metadata_path = _do_search({'product':product})


if __name__ == "__main__":
    logging.basicConfig(filename="fruit.log", level=logging.DEBUG)

    # metadata_path_parent, metadata_path
    product = 'usgs_ls8c_level1_1'
    #all_in_out_paths = list(_do_search({'product':product}))
    #print (all_in_out_paths)
    gen = _do_search({'product':product})
    print (next(gen))
    # metadata_path_parent, metadata_path, c = _do_search({'product':product})
    # #print(metadata_path_parent)
    #print(metadata_path)
