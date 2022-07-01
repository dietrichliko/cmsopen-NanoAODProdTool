"""nanoaod """
import logging
import configparser
import pathlib
import shutil
import os
import sys
from typing import cast
from typing import TextIO

import click
from nanoaodprodtool import jobs

logging.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.WARNING,
)
log = logging.getLogger(__package__)

DEFAULT_MAX_FILES = 40
OUTPUT_URL = (
    "root://eos.grid.vbc.ac.at//eos/vbc/experiments/cms/store/user/liko/NanoAODPlus/"
)
BACKUP_URL = "root://eosuser.cern.ch//eos/user/l/liko/NanoAODPlus/"

NANOAOD_CONFIGS = [
    "2010Data",
    "2010MC",
    "2011Datapp2.76",
    "2011Data",
    "2011MC",
    "2015Data",
    "2015MC",
    "2015miniMC",
]


@click.group
@click.option("-v", "--verbose", is_flag=True)
@click.option("-d", "--debug", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, verbose: str, debug: str):
    """NanoAOD job manager."""
    if debug:
        log.setLevel(logging.DEBUG)
    elif verbose:
        log.setLevel(logging.INFO)

    cfg = configparser.ConfigParser()
    if os.path.exists("nanojobs.ini"):
        cfg.read('nanojobs.ini')
    else:
        log.fatal("No nanojobs.ini in directory.")
        sys.exit()

    manager = jobs.Manager(
        pathlib.Path.cwd(), 
        cfg['nanojobs']['output'],
        cfg['nanojobs']['backup'],
        cfg['nanojobs']['singularity'],
        cfg['nanojobs']['sites'].split()
    )
    with manager:
        manager.update()

    ctx.ensure_object(dict)
    ctx.obj["manager"] = manager


@cli.command
@click.argument(
    "dataset",
)
@click.option("--dasname")
@click.option(
    "--config",
    type=click.Choice(NANOAOD_CONFIGS),
    help="NanoAODplus configuration",
    required=True,
)
@click.option(
    "--max-files",
    default=DEFAULT_MAX_FILES,
    type=click.IntRange(1),
    help="Max number of files per job",
    show_default=True,
)
@click.option(
    "--max-events", default=-1, help="Max number of events per job (default:no limit)"
)
@click.option("--force/--no-force", default=False, help="Overwrite input folder")
@click.pass_context
def create(
    ctx: click.Context,
    dataset: str,
    dasname: str,
    config: str,
    max_files: int,
    max_events: int,
    force: bool,
) -> None:
    """Create task."""
    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        task = manager.create(dataset, dasname, config, max_files, max_events, force)
        if task:
            task.prepare(force)
        else:
            log.error("Dataset %s could not be created.", dataset)


@cli.command
@click.argument("file", type=click.File(encoding="UTF-8"))
@click.option(
    "--max-files",
    default=DEFAULT_MAX_FILES,
    type=click.IntRange(1),
    help="Max number of files per job",
    show_default=True,
)
@click.option(
    "--max-events", default=-1, help="Max number of events per job (default:no limit)"
)
@click.option("--force/--no-force", default=False, help="Overwrite input folder.")
@click.pass_context
def mcreate(
    ctx: click.Context, file: TextIO, max_files: int, max_events: int, force: bool
) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        for dasname in file.read().splitlines():
            parts = dasname.rstrip().split("/")
            if parts[3] == "AOD":
                config = f"{parts[2][3:7]}Data"
                dataset = f"{parts[2][:8]}_{parts[1]}"
            elif parts[3] == "AODSIM":
                period = parts[2].split("-")[0]
                if period.startswith("Summer11"):
                    config = "2011MC"
                    dataset = f"MonteCarlo11_{period}_{parts[1]}"
                elif period.startswith("Summer12"):
                    config = "2012MC"
                    dataset = f"MonteCarlo12_{period}_{parts[1]}"
                else:
                    log.error("Cannot determine year of MC dataset %s", dasname)
                    continue
            else:
                log.error("Cannot deterime data format of %s", dasname)
                continue
            task = manager.create(
                dataset, dasname, config, max_files, max_events, force
            )
            if task:
                task.prepare(force)
            else:
                log.error("Dataset %s could not be created.", dataset)


@cli.command
@click.argument("dataset")
@click.option("--cleanup", is_flag=True)
@click.pass_context
def remove(ctx: click.Context, dataset: str, cleanup: bool) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        task = manager.tasks.get(dataset)
        if task:
            if cleanup:
                for name in ("input", "output"):
                    shutil.rmtree(
                        manager.tasks[dataset].dataset_dir / name, ignore_errors=True
                    )
            del manager.tasks[dataset]
        else:
            log.error("Dataset %s does not exists.", dataset)


@cli.command
@click.argument("dataset")
@click.option("--max-jobs", default=0, type=click.IntRange(0))
@click.pass_context
def submit(ctx: click.Context, dataset: str, max_jobs: int) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        task = manager.tasks.get(dataset)
        if task:
            task.submit(max_jobs)
        else:
            log.error("Dataset % s does not exists.", dataset)

@cli.command
@click.option("--max", default=500, type=click.IntRange(0))
@click.pass_context
def msubmit(ctx: click.Context, max: int) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        for task in manager.tasks.values():
            states = manager.states()
            cnt = states[jobs.JobState.READY] + states[jobs.JobState.RUNNING]
            if cnt < max:
                task.submit(max - cnt)
            else:
                break

@cli.command
@click.pass_context
@click.argument("dataset", default="")
def list(ctx: click.Context, dataset: str) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        if dataset:
            task = manager.tasks.get(dataset)
            if task:
                for job in task.jobs.values():
                    print(job)
            else:
                log.error("Dataset % s does not exists.", dataset)
        else:
            for task in manager.tasks.values():
                print(task)


if __name__ == "__main__":
    cli()
