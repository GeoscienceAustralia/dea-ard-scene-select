#!/usr/bin/env python3

import argparse
import logging
from datetime import datetime
from pathlib import Path
from subprocess import check_call
from typing import Set, Tuple

import psycopg2

from scene_select.collections import index_level1_path


def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    return logging.getLogger(__name__)


def generate_filesystem_paths(base_path: str, output_file: Path) -> Set[str]:
    """
    Use 'lfs find' to generate filesystem paths.
    Returns the loaded paths.
    """
    if not output_file.exists():
        logging.info(f"Generating filesystem paths file: {output_file}")
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
        logging.info(f"Generating database paths file: {output_file}")
        conn = psycopg2.connect("")  # Use standard postgres environment variables

        with conn.cursor() as cur, output_file.open("w") as f:
            cur.copy_expert(
                """
                COPY (
                    SELECT uri_body
                    FROM agdc.dataset_location
                    WHERE uri_scheme = 'file'
                    AND uri_body LIKE '///g/data/da82/AODH/USGS/L1/Landsat/C2%'
                ) TO STDOUT
            """,
                f,
            )

        conn.close()

    # Load and clean paths (remove triple slashes)
    return {"/" + path.lstrip("/") for path in load_paths(output_file)}


def generate_l1_yaml(tar_path: str, logger: logging.Logger) -> bool:
    """
    Generate missing YAML for a tar file using eo3-prepare.
    Returns True if successful.
    """
    try:
        logger.info(f"Generating YAML for {tar_path}")
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
            logger.error(f"Generated YAML does not exist: {expected_yaml}")
            return False
        return True
    except Exception as e:
        logger.error(f"Failed to generate YAML for {tar_path}: {e}")
        return False


def try_index_yaml(yaml_path: str, logger: logging.Logger) -> bool:
    """
    Index an unindexed YAML file.
    Returns True if it did so.
    """
    try:
        logger.info(f"Indexing {yaml_path}")
        was_indexed = index_level1_path(Path(yaml_path), logger)
        return was_indexed
    except Exception:
        logger.exception(f"Failed to index {yaml_path}")
        return False


def load_paths(filename: str) -> Set[str]:
    """Load paths from a file into a set."""
    with open(filename) as f:
        return {line.strip() for line in f}


def get_corresponding_yaml(tar_path: str) -> str:
    """Convert a tar path to its corresponding yaml path."""
    return tar_path.replace(".tar", ".odc-metadata.yaml")


def get_corresponding_tar(yaml_path: str) -> str:
    """Convert a yaml path to its corresponding tar path."""
    return yaml_path.replace(".odc-metadata.yaml", ".tar")


def analyze_paths(fs_paths: Set[str], db_paths: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Analyze filesystem and database paths to find mismatches.
    Returns: Tuple of (missing_yamls, unindexed_yamls)
    """
    fs_tars = {path for path in fs_paths if path.endswith(".tar")}
    fs_yamls = {path for path in fs_paths if path.endswith(".odc-metadata.yaml")}

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


def main():
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

    args = parser.parse_args()
    logger = setup_logging()

    # Set up output paths
    date_suffix = datetime.now().strftime("%Y%m%d")
    output_dir = Path(args.output_dir or ".")
    output_dir.mkdir(parents=True, exist_ok=True)

    fs_paths_file = output_dir / f"ls-paths-{date_suffix}.txt"
    db_paths_file = output_dir / f"indexed-paths-{date_suffix}.txt"

    # Generate/load paths
    fs_paths = generate_filesystem_paths(args.base_path, fs_paths_file)
    db_paths = generate_indexed_paths(db_paths_file)

    # Analyze paths
    missing_yamls, unindexed_yamls = analyze_paths(fs_paths, db_paths)

    # Report summary results
    print("\nSummary:")
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

    # Fix issues if requested
    if args.fix:
        print("\nAttempting to fix issues...")

        fixed_yamls = 0
        for tar in missing_yamls:
            if generate_l1_yaml(tar, logger):
                fixed_yamls += 1
            if args.max_count and fixed_yamls >= args.max_count:
                break
        print(f"Generated {fixed_yamls}/{len(missing_yamls)} missing YAML files")

        fixed_indexed = 0
        for yaml in unindexed_yamls:
            if try_index_yaml(yaml, logger):
                fixed_indexed += 1
            if args.max_count and fixed_indexed >= args.max_count:
                break
        print(f"Indexed {fixed_indexed}/{len(unindexed_yamls)} YAML files")


if __name__ == "__main__":
    main()
