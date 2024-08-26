#!/usr/bin/env python3
"""
ARD Dataset Merger

This script processes a bulk run of Sentinel-2 ARD data and merges it into a datacube instance.
It archives old datasets, moves new datasets into place, and indexes them in the datacube.

Usage:
    python ard_dataset_merger.py [OPTIONS] BULK_RUN_DIR

Options:
    --dry-run  Print actions without performing them
    --help     Show this message and exit
"""

import csv
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List
from uuid import UUID

import click
import structlog
from datacube import Datacube
from datacube.index import Index
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset
from datacube.ui import click as ui
from ruamel import yaml

_LOG = structlog.get_logger()

PRODUCTION_BASE = Path("/g/data/xu18/ga")
TRASH_BASE = PRODUCTION_BASE / ".trash"


def load_archive_list(csv_path: Path) -> Dict[UUID, UUID]:
    """
    The archive CSV contains a list of Level 1 paths and the existing ARD UUID to archive when we have a new ARD
    """
    level1_to_old_ard_uuids = {}
    with csv_path.open('r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 2:
                level1_to_old_ard_uuids[UUID(row[0])] = UUID(row[1])
    return level1_to_old_ard_uuids


def process_dataset(index: Index, metadata_file: Path, archive_list: Dict[UUID, UUID], dry_run: bool) -> None:
    log = _LOG.bind(metadata_file=metadata_file)
    log.info("dataset.processing")

    if not metadata_file.name.endswith('.odc-metadata.yaml'):
        log.error("dataset.error", error="Expected dataset path to be a metadata path")
        return

    dataset = load_dataset(index, metadata_file)
    if dataset is None:
        log.error("dataset.error", error="Failed to load dataset")
        return

    if index.datasets.has(dataset.id):
        log.info("dataset.exists.skipping", dataset_id=str(dataset.id))
        return

    dest_metadata_path = PRODUCTION_BASE / get_dataset_relative_path(metadata_file)
    if dest_metadata_path.exists():
        log.info("dataset.destination.exists.skipping", destination=str(dest_metadata_path))
        return

    archive_old_dataset_if_needed(index, dataset, archive_list, dry_run, log)
    move_dataset(metadata_file, dest_metadata_path, dry_run, log)
    index_dataset(index, dataset, dry_run, log)

    log.info("dataset.processing.end", dataset_id=str(dataset.id), target_path=str(dest_metadata_path))


def load_dataset(index: Index, metadata_file: Path) -> Optional[Dataset]:
    """Load a dataset from a metadata file."""
    with metadata_file.open('r') as f:
        doc = yaml.safe_load(f)

    dataset, error_msg = Doc2Dataset(index)(doc, metadata_file.as_uri())
    if dataset is None:
        raise ValueError(f"Failed to load dataset: {error_msg}")
    return dataset


def archive_old_dataset_if_needed(index: Index,
                                  dataset: Dataset,
                                  archive_list: Dict[UUID, UUID],
                                  dry_run: bool,
                                  log: structlog.BoundLogger) -> None:
    """Archive the old ARD dataset if it exists in the archive list."""
    level1_uuid = dataset.sources['level1'].id
    if level1_uuid in archive_list:
        archive_old_dataset(index, archive_list[level1_uuid], dry_run, log)


def archive_old_dataset(index: Index, old_ard_uuid: UUID, dry_run: bool, log: structlog.BoundLogger) -> None:
    """Archive an old ARD dataset and move its files to trash."""
    log = log.bind(old_ard_uuid=str(old_ard_uuid))
    log.info("dataset.archiving")

    old_dataset = index.datasets.get(old_ard_uuid)
    if old_dataset is None:
        log.warning("dataset.archive.not_found")
        return

    if not dry_run:
        index.datasets.archive([old_ard_uuid])
    else:
        log.info("dry_run.archive", dataset_id=str(old_ard_uuid))

    move_to_trash(old_dataset, dry_run=dry_run, log=log)


def move_to_trash(dataset: Dataset, dry_run: bool, log: structlog.BoundLogger) -> None:
    """Move a dataset to the trash folder."""
    source_path = dataset.local_path
    if source_path is None:
        raise ValueError("Dataset has no local path")

    if not source_path.name.endswith('.odc-metadata.yaml'):
        raise ValueError(f"Expected dataset path to be a metadata path, got: {source_path}")

    dataset_dir = source_path.parent

    trash_date = datetime.now().strftime("%Y%m%d")
    trash_path = TRASH_BASE / trash_date / dataset_dir.relative_to(PRODUCTION_BASE)

    if dry_run:
        log.info("dry_run.move_to_trash", dataset_path=str(dataset_dir), trash_path=str(trash_path))
    else:
        trash_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_dir.rename(trash_path)


def get_dataset_relative_path(metadata_file: Path) -> Path:
    """
    Get the subfolder structure for a dataset.

    This is a standardised folder structure, but can exist anywhere on disk.
    Remove the parent folders it is sitting on.

    >>> p = Path('/g/data/xu18/ga/ga_ls8c_ard_3/088/083/2024/08/08/ga_ls8c_ard_3-2-1_088083_2024-08-08_final.odc-metadata.yaml')
    >>> get_dataset_relative_path(p).as_posix()
    'ga_ls8c_ard_3/088/083/2024/08/08/ga_ls8c_ard_3-2-1_088083_2024-08-08_final.odc-metadata.yaml'
    """
    # For now, we only support s2.
    product_name = metadata_file.name.split('-')[0]

    # The root of the folder structure has the product name.
    for folder in metadata_file.parents:
        if folder.name == product_name:
            source_base_folder = folder.parent
            break
    else:
        raise ValueError(f"Failed to base product name {product_name} in {metadata_file}")

    return metadata_file.relative_to(source_base_folder)


def move_dataset(souce_md_path: Path, dest_md_path: Path, dry_run: bool, log: structlog.BoundLogger) -> None:
    """Move a dataset from source to destination."""
    dest_md_path.parent.mkdir(parents=True, exist_ok=True)

    if not dry_run:
        shutil.move(str(souce_md_path.parent), str(dest_md_path.parent))
    else:
        log.info("dry_run.move_dataset",
                 source=str(souce_md_path.parent),
                 destination=str(dest_md_path.parent))


def index_dataset(index: Index, dataset: Dataset, dry_run: bool, log: structlog.BoundLogger) -> None:
    """Index a dataset in the datacube."""
    if not dry_run:
        index.datasets.add(dataset)
    else:
        log.info("dry_run.index_dataset", dataset_id=str(dataset.id))


@click.command(help=__doc__)
@click.argument('bulk_run_dirs', nargs=-1, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--dry-run', is_flag=True, help="Print actions without performing them")
@ui.environment_option
@ui.config_option
@ui.pass_index(app_name='ard-dataset-merger')
def main(index: Index, bulk_run_dirs: List[Path], dry_run: bool) -> None:
    """Process a bulk run of ARD data and merge into datacube."""

    _LOG.info("run.start", bulk_run_dir_count=len(bulk_run_dirs), dry_run=dry_run)
    with Datacube(index=index) as dc:
        for bulk_run_dir in bulk_run_dirs:
            log = _LOG.bind(bulk_run_dir=bulk_run_dir)

            log.info("run.processing")
            pkg_dir = bulk_run_dir / "pkg"
            if not pkg_dir.exists():
                log.error("run.error", error="pkg directory not found")
                return

            archive_list = load_archive_list(bulk_run_dir / "scene-archive-list.csv")

            for product_dir in pkg_dir.iterdir():
                if product_dir.is_dir():
                    for metadata_file in product_dir.rglob("*.odc-metadata.yaml"):
                        try:
                            process_dataset(dc.index, metadata_file, archive_list, dry_run)
                        except Exception:
                            log.exception(
                                "run.error",
                                dataset_path=str(metadata_file)
                            )

            log.info("run.processing.end")


if __name__ == '__main__':
    main()
