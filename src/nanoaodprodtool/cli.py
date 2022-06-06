"""nanoaod """

import pathlib
import logging
from typing import cast
import shutil

import click
from nanoaodprodtool import jobs

logging.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.WARNING,
)
log = logging.getLogger(__name__)

OUTPUT_URL = "root://eos.grid.vbc.ac.at//eos/vbc/experiments/cms/store/user/NanoAODPlus"
BACKUP_URL = "root://eosuser.cern.ch//eos/user/l/liko/NanoAODPlus"

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


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@click.option("-d", "--debug", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, verbose: str, debug: str):
    ctx.ensure_object(dict)

    if debug:
        log.setLevel(logging.DEBUG)
    elif verbose:
        log.setLevel(logging.INFO)

    ctx.obj["manager"] = jobs.Manager(pathlib.Path.cwd(), OUTPUT_URL, BACKUP_URL)


@cli.command()
@click.argument(
    "dataset",
)
@click.option(
    "--config",
    type=click.Choice(NANOAOD_CONFIGS),
    help="NanoAODplus configuration",
    required=True,
)
@click.option(
    "--max-files",
    default=20,
    type=click.IntRange(1),
    help="Max number of files per job",
    show_default=True,
)
@click.option(
    "--max-events", default=-1, help="Max number of events per job (default:no limit)"
)
@click.option("--force/--no-force", default=False, help="Overwrite input folder.")
@click.pass_context
def create(
    ctx: click.Context,
    dataset: str,
    config: str,
    max_files: int,
    max_events: int,
    force: bool,
) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        manager.create(dataset, config, max_files, max_events)
        manager.prepare(dataset, force)


@cli.command
@click.argument("dataset")
@click.option("--cleanup", is_flag=True)
@click.pass_context
def remove(ctx: click.Context, dataset: str, cleanup: bool) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        if dataset in manager.tasks:

            if cleanup:
                for name in ("input", "output"):
                    shutil.rmtree(
                        manager.tasks[dataset].dataset_dir / name, ignore_errors=True
                    )
            del manager.tasks[dataset]

        else:
            log.error("Dataset %s does not exits.", dataset)


@cli.command
@click.argument("dataset")
@click.option("--max-jobs", default=0, type=click.IntRange(0))
@click.pass_context
def submit(ctx: click.Context, dataset: str, max_jobs: int) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        try:
            manager.submit(dataset, max_jobs)
        except KeyError:
            log.error("Dataset %s does not exits.", dataset)


@cli.command
@click.pass_context
def list(ctx: click.Context) -> None:

    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        for task in manager.tasks.values():
            print(task)


if __name__ == "__main__":
    cli()
