"""nanoaod """
import configparser
import json
import logging
import os
import pathlib
import shutil
import sys
from nanoaodprodtool import jobs
from typing import Any
from typing import cast
from typing import TextIO

import click
import ruamel.yaml


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
    "2012Data",
    "2012MC",
    "2015Data",
    "2015MC",
    "2015miniMC",
    "2015DataPoet",
    "2015MCPoet",
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
        cfg.read("nanojobs.ini")
    else:
        log.fatal("No nanojobs.ini in directory.")
        sys.exit()

    manager = jobs.Manager(
        pathlib.Path.cwd(),
        cfg["nanojobs"]["output"],
        cfg["nanojobs"]["backup"],
        cfg["nanojobs"]["singularity"],
        cfg["nanojobs"]["sites"].split(),
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
    """Create task to process a dataset."""
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
@click.option("--poet/--no-poet", default=False, help="POET Configuration")
@click.pass_context
def mcreate(
    ctx: click.Context,
    file: TextIO,
    max_files: int,
    max_events: int,
    force: bool,
    poet: bool,
) -> None:
    """Create task from a list of datasets in a file."""
    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        for dasname in file.read().splitlines():
            parts = dasname.rstrip().split("/")
            if parts[3] in ["AOD", "MINIAOD"]:
                config = f"{parts[2][3:7]}Data"
                dataset = f"{parts[2][:8]}_{parts[1]}"
            elif parts[3] in ["AODSIM", "MINIAODSIM"]:
                period = parts[2].split("-")[0]
                if period.startswith("Summer11"):
                    config = "2011MC"
                    dataset = f"MonteCarlo11_{period}_{parts[1]}"
                elif period.startswith("Summer12"):
                    config = "2012MC"
                    dataset = f"MonteCarlo12_{period}_{parts[1]}"
                elif period.startswith("RunIIFall15"):
                    config = "2015MC"
                    dataset = f"mc/{period}_{parts[1]}"
                else:
                    log.error("Cannot determine year of MC dataset %s", dasname)
                    continue
            else:
                log.error("Cannot determine data format of %s", dasname)
                continue
            if poet:
                if not config.startswith("2015"):
                    log.error("Poet only for 2015")
                    continue
                config += "Poet"
            task = manager.create(
                dataset, dasname, config, max_files, max_events, force
            )
            if task:
                task.prepare(force)
            else:
                log.error("Dataset %s could not be created.", dataset)


@cli.command
@click.argument("dataset")
@click.option("--url", default="")
@click.option("--force", is_flag=True, default=False)
@click.pass_context
def metadata(ctx: click.Context, dataset: str, url: str, force: bool) -> None:
    """Write metadata for job in metadata.yaml amd file_index.txt."""
    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        task = manager.tasks.get(dataset)
        if task is None:
            log.error("Dataset %s does not exists.", dataset)
            return
        if not force and task.state != jobs.JobState.DONE:
            log.error("Dataset %s is in state %s", dataset, task.state.name)
            return

        file_index: list[str] = []
        metadata: list[dict[str, Any]] = []

        for i, j in enumerate(task.jobs.values()):
            if j.state == jobs.JobState.DONE:
                with open(task.dataset_dir / f"output/metadata_{i}.json") as inp:
                    info = json.load(inp)
                    if url:
                        data_url = (
                            f"{url}/{task.dataset_dir.name}/{info['url'].split('/')[-1]}"
                        )
                    else:
                        data_url = info["url"]
                    file_index.append(data_url)
                    metadata.append(
                        {
                            "url": data_url,
                            "size": info["size"],
                            "entries": info["entries"],
                            "checksum": info["checksum"],
                            "parents": info["parents"],
                        }
                    )

        with open(task.dataset_dir / "output/metadata.yaml", "w") as out:
            yaml = ruamel.yaml.YAML()
            yaml.explicit_start = True  # type: ignore
            yaml.indent(sequence=4, offset=2)
            yaml.dump(
                {
                    "dataset": dataset,
                    "parent": task.dasname,
                    "cmssw": task.cmssw_dir.name,
                    "files": metadata,
                },
                out,
            )

        with open(task.dataset_dir / "output/file_index.txt", "w") as out:
            out.write("\n".join(file_index))


@cli.command
@click.argument("dataset")
@click.option("--cleanup", is_flag=True)
@click.pass_context
def remove(ctx: click.Context, dataset: str, cleanup: bool) -> None:
    """Remove a task from the DB."""
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
@click.option("--resubmit", is_flag=True, default=False)
@click.pass_context
def submit(ctx: click.Context, dataset: str, max_jobs: int, resubmit: bool) -> None:
    """Submit the jobs of a dataset to the grid."""
    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        task = manager.tasks.get(dataset)
        if task:
            task.submit(max_jobs, resubmit)
        else:
            log.error("Dataset % s does not exists.", dataset)


@cli.command
@click.option("--max", default=500, type=click.IntRange(0))
@click.pass_context
def msubmit(ctx: click.Context, max: int) -> None:
    """Submit multiple task until a maximal number of jobs is reached."""
    manager = cast(jobs.Manager, ctx.obj["manager"])
    with manager:
        for task in manager.tasks.values():
            states = manager.states()
            cnt = states[jobs.JobState.READY] + states[jobs.JobState.RUNNING]
            if cnt < max:
                task.submit(max - cnt)
            else:
                break


@cli.command("list")
@click.pass_context
@click.argument("dataset", default="")
def cli_list(ctx: click.Context, dataset: str) -> None:
    """List task and their prcessing status."""
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
