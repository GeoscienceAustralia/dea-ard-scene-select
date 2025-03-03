#!/usr/bin/env python3

from pathlib import Path
from typing import Dict, List
import re
from dataclasses import dataclass
import pandas as pd


@dataclass
class BatchStats:
    """Statistics for a batch of jobs"""

    submission_dir: str
    batch_id: str
    total_centi_service_units: int  # Service units × 100
    total_seconds_cpu_time: int
    total_seconds_walltime: int
    failed_jobs: int
    successful_jobs: int
    # Stats from batch status files
    datasets_done: int = 0
    datasets_failed: int = 0
    datasets_pending: int = 0
    datasets_running: int = 0


def parse_time_to_seconds(time_str: str) -> int:
    """Convert PBS time format (HH:MM:SS) to seconds"""
    try:
        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(float, parts)
            return int(hours * 3600 + minutes * 60 + seconds)
    except (ValueError, AttributeError):
        return 0
    return 0


def parse_pbs_output(file_path: Path) -> Dict:
    """Parse the PBS output file to extract resource usage statistics"""
    stats = {}
    try:
        with open(file_path, "r") as f:
            content = f.read()

        # Find the resource usage section
        usage_section = content.split("Resource Usage on ")[-1]

        patterns = {
            "service_units": r"Service Units:\s+(\d+\.?\d*)",
            "exit_status": r"Exit Status:\s+(\d+)",
            "cpu_time": r"CPU Time Used:\s+([^\n]+)",
            "walltime": r"Walltime Used:\s+([^\n]+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, usage_section)
            if match:
                value = match.group(1)
                if key == "service_units":
                    # Convert to centi-service-units (×100)
                    stats[key] = int(float(value) * 100)
                elif key == "exit_status":
                    stats[key] = int(value)
                elif key in ["cpu_time", "walltime"]:
                    stats[key] = parse_time_to_seconds(value)
    except (FileNotFoundError, IndexError):
        pass

    return stats


def read_batch_status(batch_dir: Path) -> Dict[str, int]:
    """Read the batch status files for done/failed/pending/running counts"""
    status_counts = {
        "datasets_done": 0,
        "datasets_failed": 0,
        "datasets_pending": 0,
        "datasets_running": 0,
    }

    status_files = {
        "datasets_done": "level-1-final_state-done.txt",
        "datasets_failed": "level-1-final_state-failed.txt",
        "datasets_pending": "level-1-final_state-pending.txt",
        "datasets_running": "level-1-final_state-running.txt",
    }

    for status, filename in status_files.items():
        try:
            file_path = batch_dir / filename
            if file_path.exists():
                with open(file_path, "r") as f:
                    status_counts[status] = sum(1 for line in f if line.strip())
        except Exception:
            pass

    return status_counts


def analyze_batch_directory(batch_dir: Path) -> BatchStats:
    """Analyze all jobs in a batch directory"""
    # Initialize batch statistics
    batch_stats = BatchStats(
        submission_dir=batch_dir.parent.name,
        batch_id=batch_dir.name,
        total_centi_service_units=0,
        total_seconds_cpu_time=0,
        total_seconds_walltime=0,
        failed_jobs=0,
        successful_jobs=0,
    )

    # Get status counts from batch text files
    status_counts = read_batch_status(batch_dir)
    for key, value in status_counts.items():
        setattr(batch_stats, key, value)

    # Process each job's PBS output
    for pbs_file in batch_dir.glob("jobid-*/*.bash.o*"):
        stats = parse_pbs_output(pbs_file)
        if stats:
            batch_stats.total_centi_service_units += stats.get("service_units", 0)
            batch_stats.total_seconds_cpu_time += stats.get("cpu_time", 0)
            batch_stats.total_seconds_walltime += stats.get("walltime", 0)
            if stats.get("exit_status", -1) == 0:
                batch_stats.successful_jobs += 1
            else:
                batch_stats.failed_jobs += 1

    return batch_stats


def analyze_submissions(submission_dirs: List[str]) -> pd.DataFrame:
    """Analyze all batches in the given submission directories"""
    all_batch_stats = []

    for submission_dir in submission_dirs:
        path = Path(submission_dir)
        if not path.exists():
            print(f"Warning: Directory not found: {submission_dir}")
            continue

        # Process each batch directory
        for batch_dir in path.glob("batchid-*"):
            batch_stats = analyze_batch_directory(batch_dir)
            all_batch_stats.append(batch_stats)

    # Convert to DataFrame
    df = pd.DataFrame([vars(stat) for stat in all_batch_stats])

    # Add derived metrics
    if not df.empty:
        df["total_datasets"] = (
            df["datasets_done"]
            + df["datasets_failed"]
            + df["datasets_pending"]
            + df["datasets_running"]
        )
        df["completion_rate"] = df["datasets_done"] / df["total_datasets"].replace(0, 1)

        # Calculate service units per dataset using centi-service-units
        df["centi_service_units_per_done_dataset"] = df[
            "total_centi_service_units"
        ] / df["datasets_done"].replace(0, 1)

        # Add hours columns for readability
        df["cpu_time_hours"] = df["total_seconds_cpu_time"] / 3600
        df["walltime_hours"] = df["total_seconds_walltime"] / 3600

        # Drop the seconds columns if you prefer to work with hours
        df = df.drop(["total_seconds_cpu_time", "total_seconds_walltime"], axis=1)

    return df


def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: script.py submission_dir1 [submission_dir2 ...]")
        sys.exit(1)

    submission_dirs = sys.argv[1:]
    print(f"Analyzing {len(submission_dirs)} submission directories...")

    df = analyze_submissions(submission_dirs)

    if df.empty:
        print("No data found in the specified directories.")
        return

    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total batches processed: {len(df)}")
    print(
        f"Total Service Units used: {df['total_centi_service_units'].sum() / 100:.2f}"
    )
    print(f"Total datasets attempted: {df['total_datasets'].sum()}")
    print(f"Total done datasets: {df['datasets_done'].sum()}")
    print(f"Average completion rate: {df['completion_rate'].mean() * 100:.1f}%")

    # Save to CSV in current dir
    output_file = "batch_processing_stats.csv"
    df.to_csv(output_file, index=False)
    print(f"\nDetailed statistics saved to {output_file}")


if __name__ == "__main__":
    main()
