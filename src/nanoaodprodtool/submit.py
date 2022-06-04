"""NanoAODProdTOOL submit.

Submit a dataset for production.
"""
import logging
import pathlib
import sys
import json

import click
import jinja2

logging.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.WARNING,
)
log = logging.getLogger(__name__)


SITES = ["T2_AT_Vienna"]

LOG_LOCATION = "/scratch/????"

SINGULARITY = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/slc6:x86_64"


@click.command
@click.argument(
    "dataset_dir",
    metavar="DIR",
    type=click.Path(
        file_okay=False, writable=True, resolve_path=True, path_type=pathlib.Path
    ),
)
def main(dataset_dir: pathlib.Path):

    input_dir = dataset_dir / "input"

    if not input_dir.exists():
        log.fatal("Dataset is no prepared")
        sys.exit()

    with open(input_dir / "job_index.json") as inp:
        task = json.load(inp)

    new_jobs = {i: j for i, j in task["jobs"].items() if j["status"] == "Ready"}

    j2_env = jinja2.Environment(loader=jinja2.PackageLoader(__package__, "templates"))
    template = j2_env.get_template("nanoaod.job.j2")

    with open(input_dir / "nanoaod.job", "w") as out:
        out.write(
            template.render(
                dataset=dataset_dir.name,
                jobscript=task["cfg"],
                release=task["cmssw"],
                jobs=new_jobs,
                log_location=LOG_LOCATION,
                sites=SITES,
                singularity=SINGULARITY,
                metadata=pathlib.Path(__file__).with_name("scripts") / "metadata",
            )
        )
