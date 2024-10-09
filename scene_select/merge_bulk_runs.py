#!/usr/bin/env python3
"""
ARD Dataset Merger

This script processes a bulk run of ARD data and merges it into a datacube instance.
It archives old datasets, moves new datasets into place, and indexes them in the datacube.

Usage:
    python ard_dataset_merger.py [OPTIONS] BULK_RUN_DIR

Options:
    --dry-run  Print actions without performing them
    --help     Show this message and exit
"""

import csv
import shutil
import sys
import time
from datetime import datetime

from pathlib import Path
from typing import Dict, Optional, List, Tuple, Iterator, Set, NamedTuple
from uuid import UUID

import click
import structlog
from datacube import Datacube
from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset
from datacube.ui import click as ui
from eodatasets3.prepare.landsat_l1_prepare import normalise_nci_symlinks
from eodatasets3.utils import default_utc
from ruamel import yaml

from scene_select.collections import get_product
from scene_select.library import ArdProduct
from scene_select.utils import structlog_setup

_LOG = structlog.get_logger()

UUIDsForPath = Dict[Path, List[UUID]]
RUN_TIMESTAMP = datetime.now()


class UnexpectedDataset(ValueError): ...


def load_archive_list(csv_path: Path) -> UUIDsForPath:
    """
    The archive CSV contains a list of Level 1 paths and the existing ARD UUIDs to archive when we have a new ARD for
    that level 1.
    """
    archive_list = {}
    with csv_path.open("r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            path, *uuid_list = row
            archive_list[normal_path(Path(path))] = [UUID(uuid) for uuid in uuid_list]
    return archive_list


def find_source_level1_path(metadata_file: Path) -> Path:
    [proc_info_file] = metadata_file.parent.glob("*.proc-info.yaml")
    with proc_info_file.open("r") as f:
        proc_info = yaml.safe_load(f)
        level1_path = Path(proc_info["wagl"]["source_datasets"]["source_level1"])
    return level1_path


def normal_path(path: Path) -> Path:
    return normalise_nci_symlinks(path.absolute())


class DatasetFilter(NamedTuple):
    only_products: Optional[Set[str]] = None
    only_region_codes: Optional[Set[str]] = None
    only_time_range: Optional[Tuple[datetime, datetime]] = None
    only_same_filesystem: bool = True

    def should_process_dataset(
        self, dataset: Dataset, collection_product: ArdProduct, log
    ) -> bool:
        if self.only_products and (dataset.product.name not in self.only_products):
            log.info("dataset.skip.excluded_product", product=dataset.product.name)
            return False

        if self.only_region_codes and (
            dataset.metadata.region_code not in self.only_region_codes
        ):
            log.info(
                "dataset.skip.excluded_region_code",
                region_code=dataset.metadata.region_code,
            )
            return False

        if self.only_time_range:
            earliest, latest = self.only_time_range
            if not (
                default_utc(earliest)
                <= default_utc(dataset.center_time)
                <= default_utc(latest)
            ):
                log.info(
                    "dataset.skip.outside_time_range", center_time=dataset.center_time
                )
                return False

        if self.only_same_filesystem and not same_filesystem(
            dataset.local_path, collection_product.base_package_directory
        ):
            log.info(
                "dataset.skip.requires_a_copy", destination=str(dataset.local_path)
            )
            return False

        return True


def process_dataset(
    index: Index,
    *,
    metadata_file: Path,
    archive_list: UUIDsForPath,
    dataset_filter: DatasetFilter = DatasetFilter(),
    dry_run: bool = False,
) -> bool:
    """
    Returns true if the dataset was processed, false if it was skipped.
    """
    log = _LOG.bind(newly_arrived_dataset=metadata_file)
    log.info("dataset.processing.start")

    if not metadata_file.name.endswith(".odc-metadata.yaml"):
        raise UnexpectedDataset(
            "Expected metadata file to be a .odc-metadata.yaml file"
        )

    dataset = load_dataset(index, metadata_file)
    source_level1_path = find_source_level1_path(metadata_file)

    if index.datasets.has(dataset.id):
        log.info("dataset.skip.already_indexed", dataset_id=str(dataset.id))
        # TODO: mark this somehow? Remove the file?
        return False

    processing_base, metadata_offset = split_dataset_base_path(metadata_file)
    collection_product = get_product(dataset.product.name)
    dest_metadata_path = collection_product.base_package_directory / metadata_offset

    if not dataset_filter.should_process_dataset(dataset, collection_product, log):
        return False

    # We are processing!
    original_metadata_file = metadata_file

    # Move to same filesystem if needed.
    metadata_file = consolidate_filesystem(
        metadata_file, dest_metadata_path, dry_run, log
    )

    # 1. Archive and trash the older datasets for the same L1.
    for old_ard_uuid in archive_list.get(source_level1_path, []):
        archive_old_dataset(index, old_ard_uuid, dry_run, log)

    # 2. Move the dataset into place
    rename_dataset(metadata_file, dest_metadata_path, dry_run, log)

    # 3. Index it.
    dataset.uris = [dest_metadata_path.as_uri()]
    index_dataset(index, dataset, dry_run, log)

    # If we had to copy to a different filesystem, we can trash the one on the old filesystem.
    if metadata_file != original_metadata_file:
        move_to_trash(original_metadata_file, dry_run, log)

    log.info(
        "dataset.processing.end",
        dataset_id=str(dataset.id),
        target_path=str(dest_metadata_path),
    )
    return True


def consolidate_filesystem(
    metadata_file: Path, eventual_destination_path: Path, dry_run, log
) -> Path:
    destination_base, _ = split_dataset_base_path(eventual_destination_path)
    if not same_filesystem(metadata_file, destination_base):
        base_path, metadata_offset = split_dataset_base_path(metadata_file)

        # Place it on the same filesystem as the eventual destination.
        inbox_metadata_path = (
            destination_base
            / ".inbox"
            / RUN_TIMESTAMP.strftime("%Y%m%d-%H%M%S")
            / metadata_offset
        )
        if inbox_metadata_path.exists():
            log.warning("dataset.skip.already_in_inbox", path=inbox_metadata_path)
            raise UnexpectedDataset(
                f"Dataset {metadata_file} is already in inbox: {inbox_metadata_path}"
            )
        source_dataset_dir = metadata_file.parent
        destination_dataset_dir = inbox_metadata_path.parent

        log = log.bind(
            source=source_dataset_dir,
            destination=destination_dataset_dir,
            mkdir=destination_dataset_dir.parent,
        )
        log.info("do.dest_filesystem_copy")
        if not dry_run:
            destination_dataset_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_dataset_dir, destination_dataset_dir)

            # Sanity check
            if not inbox_metadata_path.exists():
                raise UnexpectedDataset(
                    f"Dataset is not in the expected inbox? {inbox_metadata_path}"
                )
            log.info("do.dest_filesystem_copy.done")
        metadata_file = inbox_metadata_path

    return metadata_file


def load_dataset(index: Index, metadata_file: Path, with_lineage=True) -> Dataset:
    """
    Load a dataset from a metadata file.
    """
    with metadata_file.open("r") as f:
        doc = yaml.safe_load(f)

    # Note that this fails if the lineage cannot be found.
    dataset, error_msg = Doc2Dataset(
        index, verify_lineage=True, skip_lineage=not with_lineage
    )(doc, metadata_file.as_uri())
    if dataset is None:
        raise ValueError(f"Failed to load dataset: {error_msg}")
    return dataset


def archive_old_dataset(
    index: Index,
    old_ard_uuid: UUID,
    dry_run: bool,
    log: structlog.BoundLogger,
) -> None:
    """Archive an old ARD dataset and move its files to trash."""
    log = log.bind(old_ard_uuid=str(old_ard_uuid))

    old_dataset = index.datasets.get(old_ard_uuid)
    if old_dataset is None:
        log.warning("dataset.archive.not_found")
        return

    was_already_archived = old_dataset.is_archived
    if was_already_archived:
        log.info("dataset.already_archived", archive_time=old_dataset.archived_time)
    else:
        log.info("do.archive_in_index")
        if not dry_run:
            index.datasets.archive([old_ard_uuid])

    move_to_trash(
        old_dataset.local_path,
        dry_run=dry_run,
        log=log,
        # If it was already archived, check that this isn't a different dataset.
        expected_dataset_id=old_dataset.id if was_already_archived else None,
    )


def _normalise(path: Path) -> Path:
    return normalise_nci_symlinks(path.absolute())


def move_to_trash(
    source_path: Path,
    dry_run: bool,
    log: structlog.BoundLogger,
    expected_dataset_id: Optional[UUID] = None,
) -> None:
    """Move a dataset to the trash folder."""
    if source_path is None:
        raise ValueError("Dataset has no local path")

    log = log.bind(metadata_path=source_path)
    if not source_path.name.endswith(".odc-metadata.yaml"):
        raise ValueError(
            f"Expected dataset path to be a metadata path, got: {source_path}"
        )
    if not source_path.exists():
        log.info("dataset.trash.already_gone")
        return

    # Load the yaml file to check if the dataset was different.
    if expected_dataset_id:
        disk_id = _load_dataset_uuid_from_disk(source_path)
        if disk_id != str(expected_dataset_id):
            log.warning(
                "dataset.trash.different_dataset",
                our_dataset_id=str(expected_dataset_id),
                disk_dataset_id=disk_id,
            )
            # We're still going to trash it, since we're moving a new dataset here.
        else:
            log.info("dataset.trash.same_dataset")

    dataset_dir = source_path.parent

    base_path, metadata_offset = split_dataset_base_path(source_path)
    trash_path = (
        base_path / ".trash" / RUN_TIMESTAMP.strftime("%Y%m%d") / metadata_offset.parent
    )

    log.info(
        "do.move_to_trash",
        dataset_path=str(dataset_dir),
        trash_path=str(trash_path),
    )
    if not dry_run:
        trash_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_dir.rename(trash_path)


def _load_dataset_uuid_from_disk(source_path: Path) -> str:
    """
    Get the dataset ID from a metadata file. Note that this will be slow so we don't want to do it often...
    """
    with source_path.open("r") as f:
        doc = yaml.safe_load(f)
    return str(doc["id"])


def get_nci_drive(p: Path) -> Path:
    """
    What storage area is this on?

    Files should be safely rename'able within a storage area.

    >>> get_nci_drive(Path('/g/data/xu18/ga/ga_ls8c_ard_3/088/083/2024/08/08/ga_ls8c_ard_3-2-1_088083_2024-08-08_final.odc-metadata.yaml'))
    PosixPath('/g/data/xu18')
    >>> get_nci_drive(Path('/scratch/v10/lpgs/some-offshore-procs.txt'))
    PosixPath('/scratch/v10')
    >>> get_nci_drive(Path('/home/547/lpgs/dea-orchestration'))
    PosixPath('/home/547/lpgs')
    """
    match normalise_nci_symlinks(p.absolute()).parts:
        case ("/", "g", "data", project, *_):
            return Path("/g/data", project)
        case ("/", "scratch", project, *_):
            return Path("/scratch", project)
        case ("/", "home", section, user, *_):
            return Path("/home", section, user)
        case _:
            raise ValueError(f"Unknown NCI drive structure: {p}")


def same_filesystem(path1: Path, path2: Path) -> bool:
    """ "
    Are the two paths on the same filesystem?

    ie. can we rename() one to the other without copying it?

    """
    # Sadly st_dev is not accurate within lustre.
    #  os.stat('/g/data/v10/').st_dev == os.stat('/g/data/xu18/').st_dev

    # Instead we'll do a very-nci-specific thing.
    path1_drive = get_nci_drive(path1)
    path2_drive = get_nci_drive(path2)
    return path1_drive == path2_drive


def split_dataset_base_path(metadata_file: Path) -> Tuple[Path, Path]:
    """
    Get the subfolder structure for a dataset.

    This is a standardised folder structure, but can exist anywhere on disk.
    Remove the parent folders it is sitting on.

    >>> p = Path('/g/data/xu18/ga/ga_ls8c_ard_3/088/083/2024/08/08/ga_ls8c_ard_3-2-1_088083_2024-08-08_final.odc-metadata.yaml')
    >>> base, ds = split_dataset_base_path(p)
    >>> ds.as_posix()
    'ga_ls8c_ard_3/088/083/2024/08/08/ga_ls8c_ard_3-2-1_088083_2024-08-08_final.odc-metadata.yaml'
    >>> base.as_posix()
    '/g/data/xu18/ga'
    >>> (base / ds) == p
    True
    """
    if not metadata_file.name.endswith(".odc-metadata.yaml"):
        raise ValueError(
            f"Expected dataset path to be a metadata path, got: {metadata_file}"
        )

    product_name = metadata_file.name.split("-")[0]

    # The root of the folder structure has the product name.
    for folder in metadata_file.parents:
        if folder.name == product_name:
            source_base_folder = folder.parent
            break
    else:
        raise ValueError(
            f"Failed to base product name {product_name} in {metadata_file}"
        )

    return source_base_folder, metadata_file.relative_to(source_base_folder)


def rename_dataset(
    source_md_path: Path, dest_md_path: Path, dry_run: bool, log: structlog.BoundLogger
) -> None:
    """Move a dataset from source to destination."""
    source_dataset_dir = source_md_path.parent
    dest_dataset_dir = dest_md_path.parent

    # We can only do an atomic rename on the same drive.
    # The destination doesn't dataset exist yet, so we check its base directory drive.
    if not dry_run:
        destination_base, _ = split_dataset_base_path(dest_md_path)
        if not same_filesystem(source_dataset_dir, destination_base):
            raise ValueError(
                f"Source and destination are not on the same filesystem. "
                f"(for now this script has been altered to not do moves, only renames): "
                f"{source_dataset_dir} != {destination_base}"
            )

    log.info(
        "do.rename_dataset",
        source=source_dataset_dir,
        destination=dest_dataset_dir,
        mkdir=dest_dataset_dir.parent,
    )
    if not dry_run:
        dest_dataset_dir.parent.mkdir(parents=True, exist_ok=True)
        source_dataset_dir.rename(dest_dataset_dir)


def index_dataset(
    index: Index, dataset: Dataset, dry_run: bool, log: structlog.BoundLogger
) -> None:
    """Index a dataset in the datacube."""

    log.info(
        "do.index_dataset",
        dataset_id=str(dataset.id),
        dataset_uris=dataset.uris,
    )
    if not dry_run:
        index.datasets.add(dataset)


@click.command(help=__doc__)
@click.argument(
    "bulk_run_dirs",
    nargs=-1,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option("--dry-run", is_flag=True, help="Print actions without performing them")
@ui.environment_option
@ui.config_option
@click.option(
    "--only-products",
    help="Only process specified output products",
    multiple=True,
    type=str,
)
@click.option(
    "--only-region-codes",
    help="Only process specified region codes",
    multiple=True,
    type=str,
)
@click.option(
    "--only-region-code-file",
    help="Only process region codes listed in text file (one per line)",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--only-time-range",
    help="Only process datasets between these two dates",
    nargs=2,
    type=click.DateTime(),
)
@click.option(
    "--max-count",
    type=int,
    default=sys.maxsize,
    help="Maximum number of datasets to process",
)
@click.option(
    "--max-consecutive-failures",
    type=int,
    default=5,
    help="Stop if we reach this many consecutive failures",
)
@click.option(
    "--only-same-filesystem/--allow-different-filesystems",
    is_flag=True,
    default=False,
    help="Only process datasets that don't require copying between filesystems",
)
@ui.pass_index(app_name="ard-dataset-merger")
def cli(
    index: Index,
    bulk_run_dirs: List[Path],
    dry_run: bool,
    max_count: int,
    max_consecutive_failures: int,
    only_products: Optional[Tuple[str, ...]] = None,
    only_region_codes: Optional[Tuple[str, ...]] = None,
    only_region_code_file: Optional[Path] = None,
    only_time_range: Optional[Tuple[datetime, datetime]] = None,
    only_same_filesystem: bool = True,
) -> None:
    """Process a bulk run of ARD data and merge into datacube."""
    structlog_setup()

    limit_to_region_codes = None
    if only_region_codes or only_region_code_file:
        limit_to_region_codes = set(only_region_codes) if only_region_codes else set()
        if only_region_code_file:
            with open(only_region_code_file, "r") as f:
                limit_to_region_codes.update(line.strip() for line in f.readlines())

    dataset_filter = DatasetFilter(
        only_products=(set(only_products) if only_products else None),
        only_region_codes=limit_to_region_codes,
        only_time_range=only_time_range,
        only_same_filesystem=only_same_filesystem,
    )
    count = 0
    consecutive_failures = 0
    _LOG.info("command.start", bulk_run_dir_count=len(bulk_run_dirs), dry_run=dry_run)
    with Datacube(index=index) as dc:
        for bulk_run_dir in bulk_run_dirs:
            bulk_run_dir = bulk_run_dir.resolve()

            log = _LOG.bind(bulk_run_dir=bulk_run_dir)
            log.info("scan.directory.start")

            archive_list = load_archive_list(bulk_run_dir / "scene-archive-list.csv")

            for metadata_path in iter_output_datasets(bulk_run_dir, log):
                if not metadata_path.exists():
                    log.debug(
                        "dataset.skip.already_processed", metadata_path=metadata_path
                    )
                    continue

                try:
                    was_processed = process_dataset(
                        dc.index,
                        metadata_file=metadata_path,
                        archive_list=archive_list,
                        dataset_filter=dataset_filter,
                        dry_run=dry_run,
                    )
                    consecutive_failures = 0
                    if was_processed:
                        count += 1
                        if count >= max_count:
                            log.info("scan.max_count_reached")
                            return

                except Exception as e:
                    log.exception("run.error", dataset_path=str(metadata_path))
                    consecutive_failures += 1

                    # Wait a bit before retrying
                    time.sleep(min((consecutive_failures**3) * 5, 500))
                    if consecutive_failures > max_consecutive_failures:
                        raise RuntimeError(
                            f"Reached maximum consecutive failures ({max_consecutive_failures})"
                        ) from e

            log.info("scan.directory.end")


def iter_output_datasets(bulk_run_dir: Path, log) -> Iterator[Path]:
    """Get all reported output datasets for batches that have finished for this bulk-run.

    It will return the metadata path to each dataset.
    """
    for batch_dir in bulk_run_dir.glob("batchid-*"):
        if not (batch_dir / "level-1-final_state-done.txt"):
            log.info("skipping_unfinished_batch", batch_dir=batch_dir)
            continue
        for index_file in batch_dir.glob("*-datasets-to-index.txt"):
            with open(index_file) as f:
                for line in f:
                    yield Path(line.strip())


if __name__ == "__main__":
    cli()
