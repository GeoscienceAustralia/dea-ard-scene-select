#!/usr/bin/env python3

import argparse
import time

import structlog
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import check_call
from typing import Set, Tuple

import psycopg2

from scene_select.collections import index_level1_path
from scene_select.utils import structlog_setup

_LOG = structlog.get_logger()


def generate_filesystem_paths(base_path: str, output_file: Path) -> Set[str]:
    """
    Use 'lfs find' to generate filesystem paths.
    Returns the loaded paths.
    """
    if not output_file.exists():
        _LOG.info("scanning_filesystem", output=output_file)
        with output_file.open("w") as f:
            check_call(
                [
                    "lfs",
                    "find",
                    "-type",
                    "f",
                    base_path,
                ],
                stdout=f,
            )

    return load_paths(output_file)


def generate_indexed_paths(output_file: Path) -> Set[str]:
    """
    Query database for indexed paths.
    Returns the loaded paths.
    """
    if not output_file.exists():
        _LOG.info("scanning_db", output=output_file)
        conn = psycopg2.connect("")  # Use standard postgres environment variables

        with conn.cursor() as cur, output_file.open("w") as f:
            cur.copy_expert(
                """
                COPY (
                    SELECT uri_body
                    FROM agdc.dataset_location
                    inner join agdc.dataset d on d.id = dataset_location.dataset_ref
                    WHERE uri_scheme = 'file'
                    and d.archived is null
                    AND uri_body LIKE '///g/data/da82/AODH/USGS/L1/Landsat/C2%'
                ) TO STDOUT
            """,
                f,
            )

        conn.close()

    # Load and clean paths (remove triple slashes)
    return {"/" + path.lstrip("/") for path in load_paths(output_file)}


def generate_l1_yaml(tar_path: str, log) -> bool:
    """
    Generate missing YAML for a tar file using eo3-prepare.
    Returns True if successful.
    """
    log = log.bind(tar_path=tar_path)
    try:
        log.info("generate_yaml")
        check_call(
            [
                "eo3-prepare",
                "landsat-l1",
                "--producer",
                "usgs.gov",
                tar_path,
            ]
        )
        expected_yaml = Path(get_corresponding_yaml(tar_path))
        if not expected_yaml.exists():
            log.error("expected_yaml", expected_yaml=expected_yaml)
            return False
        return True
    except Exception:
        log.error("fail_yaml_generate")
        return False


def try_index_yaml(yaml_path: str, log) -> bool:
    """
    Index an unindexed YAML file.
    Returns True if it did so.
    """
    log = log.bind(yaml_path=yaml_path)
    try:
        log.info("triggering_indexing")
        was_indexed = index_level1_path(Path(yaml_path), log)
        return was_indexed
    except Exception:
        log.exception("index_failure")
        return False


def load_paths(filename: str, exclude_string: str = "batchDownload") -> Set[str]:
    """Load paths from a file into a set."""
    with open(filename) as f:
        return {line.strip() for line in f if exclude_string not in line}


def get_corresponding_yaml(tar_path: str) -> str:
    """Convert a tar path to its corresponding yaml path."""
    return tar_path.replace(".tar", ".odc-metadata.yaml")


def get_corresponding_tar(yaml_path: str) -> str:
    """Convert a yaml path to its corresponding tar path."""
    return yaml_path.replace(".odc-metadata.yaml", ".tar")


def get_processing_date(yaml_path: str) -> datetime:
    """
    Extract acquisition date from filename.

    >>> get_processing_date('/g/data/da82/AODH/USGS/L1/Landsat/C2/084_082/LO80840822019355/LO08_L1TP_084082_20191221_20200924_02_T1.odc-metadata.yaml')
    datetime.datetime(2020, 9, 24, 0, 0)
    """
    filename = Path(yaml_path).name
    date_str = filename.split("_")[4]
    return datetime.strptime(date_str, "%Y%m%d")


def analyze_paths(
    fs_paths: Set[str], db_paths: Set[str], start_date: datetime, end_date: datetime
) -> Tuple[Set[str], Set[str]]:
    """
    Analyze filesystem and database paths to find mismatches.
    Returns: Tuple of (missing_yamls, unindexed_yamls)
    """
    fs_tars = {path for path in fs_paths if path.endswith(".tar")}
    fs_yamls = {path for path in fs_paths if path.endswith(".odc-metadata.yaml")}

    # Filter by date range
    fs_tars = {
        path for path in fs_tars if start_date <= get_processing_date(path) <= end_date
    }
    fs_yamls = {
        path for path in fs_yamls if start_date <= get_processing_date(path) <= end_date
    }

    missing_yamls = {
        tar for tar in fs_tars if get_corresponding_yaml(tar) not in fs_yamls
    }

    unindexed_yamls = fs_yamls - db_paths

    return missing_yamls, unindexed_yamls


def write_results_to_file(filename, paths: Set[str]) -> None:
    """Write a set of paths to a file."""
    with open(filename, "w") as f:
        for path in sorted(paths):
            f.write(f"{path}\n")


def main(minimum_age_hours=24):
    parser = argparse.ArgumentParser(
        description="Check and fix consistency between filesystem and database metadata"
    )
    parser.add_argument(
        "--base-path",
        default="/g/data/da82/AODH/USGS/L1/Landsat/C2",
        help="Base path for Landsat data",
    )
    parser.add_argument("--output-dir", "-o", help="Directory to write results")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt to fix missing YAMLs and index unindexed files",
    )
    parser.add_argument(
        "--max-count",
        type=int,
        help="Maximum number of files to fix",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=(datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
        help="Start date for processing (YYYY-MM-DD). Defaults to 6 months ago",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for processing (YYYY-MM-DD). Defaults to today",
    )

    args = parser.parse_args()
    structlog_setup()

    log = _LOG

    # Set up output paths
    date_suffix = datetime.now().strftime("%Y%m%d")
    output_dir = Path(args.output_dir or ".")
    output_dir.mkdir(parents=True, exist_ok=True)

    fs_paths_file = output_dir / f"ls-paths-{date_suffix}.txt"
    db_paths_file = output_dir / f"indexed-paths-{date_suffix}.txt"

    # Generate/load paths
    fs_paths = generate_filesystem_paths(args.base_path, fs_paths_file)
    db_paths = generate_indexed_paths(db_paths_file)

    # Analyze paths with date filtering
    missing_yamls, unindexed_yamls = analyze_paths(
        fs_paths, db_paths, args.start_date, args.end_date
    )

    # Report summary results
    print("\nSummary:")
    print(f"Date range: {args.start_date.date()} to {args.end_date.date()}")
    print(f"Total tar files missing yaml files: {len(missing_yamls)}")
    if missing_yamls:
        print("\nFirst 5 examples of tar files missing yaml files:")
        for path in sorted(missing_yamls)[:5]:
            print(f"  {path}")

    print(f"\nTotal yaml files not indexed in database: {len(unindexed_yamls)}")
    if unindexed_yamls:
        print("\nFirst 5 examples of unindexed yaml files:")
        for path in sorted(unindexed_yamls)[:5]:
            print(f"  {path}")

    # Write full results to files
    if missing_yamls:
        missing_file = output_dir / f"missing-yamls-{date_suffix}.txt"
        write_results_to_file(missing_file, missing_yamls)
        print(f"\nFull list of missing yaml files written to: {missing_file}")

    if unindexed_yamls:
        unindexed_file = output_dir / f"unindexed-yamls-{date_suffix}.txt"
        write_results_to_file(unindexed_file, unindexed_yamls)
        print(f"Full list of unindexed yaml files written to: {unindexed_file}")

    now = time.time()

    # Fix issues if requested
    if args.fix:
        print("\nAttempting to fix issues...")

        fixed_yamls = 0
        fixed_indexed = 0
        for tar_path in missing_yamls:
            file_age_hours = _seconds_to_hours(now - Path(tar_path).stat().st_mtime)
            if file_age_hours <= minimum_age_hours:
                log.info(
                    "skip.too_recent",
                    tar_path=tar_path,
                    file_age_hours=file_age_hours,
                    minimum_age_hours=minimum_age_hours,
                )
                continue
            if generate_l1_yaml(tar_path, log):
                fixed_yamls += 1
                expected_yaml = get_corresponding_yaml(tar_path)
                if try_index_yaml(expected_yaml, log):
                    fixed_indexed += 1
            if args.max_count and fixed_yamls >= args.max_count:
                break
        print(f"Generated {fixed_yamls}/{len(missing_yamls)} missing YAML files")

        for yaml_path in unindexed_yamls:
            if try_index_yaml(yaml_path, log):
                fixed_indexed += 1
            if args.max_count and fixed_indexed >= args.max_count:
                break
        print(f"Indexed {fixed_indexed}/{len(unindexed_yamls)} YAML files")


def _seconds_to_hours(seconds):
    return seconds / (60 * 60)


if __name__ == "__main__":
    main()
