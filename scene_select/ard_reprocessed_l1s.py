#!/usr/bin/env python3
"""
Stub file
This module is where the reprocessing code will go
"""

from pathlib import Path
import uuid

from scene_select.ard_scene_select import do_ard
from scene_select.dass_logs import LOGGER, LogMainFunction

import click



DIR_TEMPLATE = "reprocess-jobid-{jobid}"

@click.command()

@click.option(
    "--config",
    type=click.Path(dir_okay=False, file_okay=True),
    help="Full path to a datacube config text file."
    " This describes the ODC database.",
    default=None,
)
@click.option(
    "--products",
    cls=PythonLiteralOption,
    type=list,
    help="List the ODC products to be processed. e.g."
    ' \'["ga_ls5t_level1_3", "usgs_ls8c_level1_1"]\'',
    default=PRODUCTS,
)
@click.option(
    "--workdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output working directory.",
    default=Path.cwd(),
)
@click.option(
    "--brdfdir",
    type=click.Path(file_okay=False),
    help="The home directory of BRDF data used by scene select.",
    default=BRDF_DIR,
)
@click.option(
    "--wvdir",
    type=click.Path(file_okay=False),
    help="The home directory of water vapour data used by scene select.",
    default=WV_DIR,
)
@click.option(
    "--scene-limit",
    default=1000,
    type=int,
    help="Safety limit: Maximum number of scenes to process in a run. \
Does not work for multigranule zip files.",
)
@click.option(
    "--run-ard",
    default=False,
    is_flag=True,
    help="Produce ARD scenes by executing the ard_pbs script.",
)
# These are passed on to ard processing
@click.option(
    "--test",
    default=False,
    is_flag=True,
    help="Test job execution (Don't submit the job to the PBS queue).",
)
@click.option(
    "--log-config",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
    default=DATA_DIR.joinpath(LOG_CONFIG_FILE),
    help="full path to the logging configuration file",
)
@click.option(
    "--yamls-dir",
    type=click.Path(file_okay=False),
    default="",
    help="The base directory for level-1 dataset documents.",
)
@click.option("--stop-logging", default=False, is_flag=True, help="No logs.")
@click.option("--walltime", help="Job walltime in `hh:mm:ss` format.")
@click.option("--email", help="Notification email address.")
@click.option("--project", default="v10", help="Project code to run under.")
@click.option(
    "--logdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base logging and scripts output directory.",
)
@click.option(
    "--pkgdir",
    type=click.Path(file_okay=False, writable=True),
    help="The base output packaged directory.",
)
@click.option(
    "--env",
    type=click.Path(exists=True, readable=True),
    help="Environment script to source.",
)
@click.option(
    "--index-datacube-env",
    type=click.Path(exists=True, readable=True),
    help="Path to the datacube indexing environment. "
    "Add this to index the ARD results.  "
    "If this option is not defined the ARD results "
    "will not be automatically indexed.",
)
@click.option(
    "--workers",
    type=click.IntRange(1, 48),
    help="The number of workers to request per node.",
)
@click.option("--nodes", help="The number of nodes to request.")
@click.option("--memory", help="The memory in GB to request per node.")
@click.option("--jobfs", help="The jobfs memory in GB to request per node.")
@LogMainFunction()
def ard_reprocessed_l1s(
    usgs_level1_files: click.Path,
    allowed_codes: click.Path,
    config: click.Path,
    products: list,
    logdir: click.Path,
    brdfdir: click.Path,
    wvdir: click.Path,
    stop_logging: bool,
    log_config: click.Path,
    scene_limit: int,
    interim_days_wait: int,
    days_to_exclude: list,
    run_ard: bool,
    find_blocked: bool,
    **ard_click_params: dict,
):
    """
    The keys for ard_click_params;
        test: bool,
        workdir: click.Path,
        pkgdir: click.Path,
        env: click.Path,
        workers: int,
        nodes: int,
        memory: int,
        jobfs: int,
        project: str,
        walltime: str,
        email: str

    :return: list of scenes to ARD process
    """
    # pylint: disable=R0913, R0914
    # R0913: Too many arguments
    # R0914: Too many local variables

    logdir = Path(logdir).resolve()
    # If we write a file we write it in the job dir
    # set up the scene select job dir in the log dir
    jobdir = logdir.joinpath(DIR_TEMPLATE.format(jobid=uuid.uuid4().hex[0:6]))
    jobdir.mkdir(exist_ok=True)

    # FIXME test this
    if not stop_logging:
        gen_log_file = jobdir.joinpath(GEN_LOG_FILE).resolve()
        fileConfig(
            log_config,
            disable_existing_loggers=False,
            defaults={"genlogfilename": str(gen_log_file)},
        )
    LOGGER.info("scene_select", **locals())

if __name__ == "__main__":
    ard_reprocessed_l1s()
