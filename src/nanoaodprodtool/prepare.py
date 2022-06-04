"""NanoAODProdTOOL prepare.

Prepare a dataset for production.
"""

import logging
import pathlib
import shutil
import json
import sys
import tarfile
import os
import re

import click
import jinja2

logging.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.WARNING,
)
log = logging.getLogger(__name__)

FRAGMENT1 = """
import os
files = open("file_index.txt", "r").read().splitlines()
f = int( os.environ["FIRST_FILE"])
l = int( os.environ["FIRST_FILE"])
process.source = cms.Source("PoolSource",
  fileNames = cms.untracked.vstring(files[f:l])
)
"""

FRAGMENT2 = """
                outFile = cms.string('output.root'),
"""

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

SANDBOX_DIRS = ["bin", "cfipython", "config", "lib", "module", "python"]
SANDBOX_SRC_DIRS = ["data", "interface", "python"]

OUTPUT = "root://eos.grid.vbc.ac.at//eos/vbc/experiments/cms/sture/user/liko/nanoaod"


@click.command
@click.argument(
    "dataset_dir",
    metavar="DIR",
    type=click.Path(
        file_okay=False, writable=True, resolve_path=True, path_type=pathlib.Path
    ),
)
@click.option(
    "--config",
    type=click.Choice(NANOAOD_CONFIGS),
    help="NanoAODplus configuration",
    required=True,
)
@click.option("--force/--no-force", default=False)
@click.option("--max-files", default=20, type=click.IntRange(1))
@click.option(
    "--log-level",
    metavar="LVL",
    default="WARNING",
    type=click.Choice(["WARNING", "INFO", "DEBUG"]),
)
def main(
    dataset_dir: pathlib.Path, config: str, force: bool, max_files: int, log_level: str
):
    """Preparse dataset for processing."""

    log.setLevel(getattr(logging, log_level))

    log.info("Preparing %s", dataset_dir.name)
    input_dir = dataset_dir / "input"
    if input_dir.exists():
        if force:
            try:
                shutil.rmtree(input_dir)
            except Exception as e:
                log.fatal("Error 5s removing %s", e, input_dir)
                sys.exit(1)
        else:
            log.fatal("Datset %s is already prepared", dataset_dir.name)
            sys.exit(1)

    cmssw_release = find_cmssw_release(dataset_dir)

    input_dir.mkdir()

    prepare_indices(input_dir, max_files, config, cmssw_release)
    prepare_sandbox(input_dir, cmssw_release)
    prepare_config(input_dir, config, cmssw_release)
    prepare_jobscript(input_dir, config, cmssw_release)


def find_cmssw_release(dataset_dir: pathlib.Path) -> str:

    try:
        dir = next(dataset_dir.parent.glob("CMSSW_*"))
    except StopIteration:
        log.error("No CMSSSW area found.")
        sys.exit()

    log.debug("CMSSW release %s", dir.name)
    return dir.name


def prepare_indices(
    input_dir: pathlib.Path, max_files: int, config: str, cmssw_release: str
) -> None:

    files = []
    for path in input_dir.parent.iterdir():
        if path.is_file() and path.name.endswith("file_index.txt"):
            files += open(path).readlines()

    log.info("Writing %s", input_dir / "file_index.txt")
    with open(input_dir / "file_index.txt", "w") as out:
        out.writelines(files)

    nr = len(files)
    log.debug("Number of file %s", nr)
    n = int(nr / max_files)
    k, m = divmod(nr, n)

    task = {
        "cfg": f"nanoanalyzer_cfg_{config}.py",
        "cmssw": cmssw_release,
        "jobs": { 
            i: {
                "status": "Ready",
                "first": i * k + min(i, m),
                "last": (i + 1) * k + min(i + 1, m),
            }
            for i in range(n)
        },
    }
    log.info("Writing %s", input_dir / "job_index.json")
    with open(input_dir / "job_index.json", "w") as out:
        json.dump(task, out)
    log.debug("Number of jobs %s", n)


def prepare_sandbox(input_dir: pathlib.Path, cmssw_release: str):

    log.info("Writing %s", input_dir / "sandbox.tar.bz2")
    tarball = tarfile.open(input_dir / "sandbox.tar.bz2", "w|bz2")

    local_dir = input_dir.parent.parent / cmssw_release

    for dir in SANDBOX_DIRS:
        path = local_dir / dir
        if path.exists():
            tarball.add(path)

    for root, _filenames, dirnames in os.walk(local_dir / "src"):
        for name in dirnames:
            if name in SANDBOX_SRC_DIRS:
                tarball.add(os.path.join(root, name))

    tarball.close()


def prepare_config(input_dir: pathlib.Path, config: str, cmssw_release: str) -> None:

    inp_cfg = (
        input_dir.parent.parent
        / f"{cmssw_release}/src/NanoAOD/NanoAnalyzer/nanoanalyzer_cfg_{config}.py"
    )
    out_cfg = input_dir / f"nanoanalyzer_cfg_{config}.py"

    log.info("Writing %s", out_cfg)
    re_source = re.compile(r"\s*process\.source\s*=\s*cms\.Source.*")
    re_end = re.compile(r"\s*\).*")
    re_out = re.compile(r"\s*outFile\s*=\s*cms.string")

    with open(inp_cfg, "r") as inp, open(out_cfg, "w") as out:
        state = 0
        for i, line in enumerate(inp.readlines()):
            if state == 0:
                if match := re_source.match(line):
                    log.debug("Line %3d: Found input", i)
                    state = 1
                    out.write(FRAGMENT1)
                elif match := re_out.match(line):
                    log.debug("Line %3d: Found output", i)
                    out.write(FRAGMENT2)
                else:
                    out.write(line)
            elif state == 1:
                match = re_end.match(line)
                if match:
                    log.debug("Line %3d: End input", i)
                    state = 0
        log.debug("Line %3d: EOF", i)


def prepare_jobscript(input_dir: pathlib.Path, config: str, cmssw_release: str) -> None:

    j2_env = jinja2.Environment(loader=jinja2.PackageLoader(__package__, "templates"))

    template = j2_env.get_template("nanoaod.sh.j2")

    with open(input_dir / "nanoaod.sh", "w") as out:
        out.write(
            template.render(
                output=f"{OUTPUT}/{input_dir.parent.name}",
                jobscript=f"nanoanalyzer_cfg_{config}.py",
                release=cmssw_release,
            )
        )
