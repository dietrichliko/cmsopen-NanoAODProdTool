"""A job implemented as FSM."""

import enum
import pathlib
import logging
import pickle
import os
import shutil
import tarfile
import re
import sys
from types import TracebackType
import subprocess

import finite_state_machine as fsm
import jinja2

log = logging.getLogger(__name__)
j2_env = jinja2.Environment(loader=jinja2.PackageLoader(__package__, "templates"))

SANDBOX_DIRS = ["bin", "cfipython", "config", "lib", "module", "python"]
SANDBOX_SRC_DIRS = ["data", "interface", "python"]

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

FRAGMENT3 = """
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({}))
"""


SITES = ["T2_AT_Vienna"]

LOG_LOCATION = f"/scratch/{os.getlogin()}"

SINGULARITY = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/slc6:x86_64"

PACKAGE_DIR = pathlib.Path(__file__).parent


class JobState(enum.Enum):
    """Job states."""

    IDLE = 1
    READY = 2
    SUBMITTED = 3
    RUNNING = 4
    FINISHED = 5
    DONE = 6
    ERROR = 7


class Job(fsm.StateMachine):
    """A job for NanoAODPlusProduction."""

    state: JobState
    nr: int
    first_file: int
    last_file: int
    htc_cluster: int
    htc_process: int
    htc_state: str

    def __init__(self, nr: int, first_file: int, last_file: int) -> None:
        self.status = JobState.READY
        self.nr = nr
        self.first_file = first_file
        self.last_file = last_file

        self.htc_cluster = -1
        self.htc_process = -1
        self.htc_state = ""

    @fsm.transition(JobState.READY, JobState.SUBMITTED)
    def submit(self, cluster: int, process: int) -> None:
        self.htc_cluster = cluster
        self.htc_process = process


class Task(fsm.StateMachine):
    """A taks is a dataset for NanoAODPlusProduction."""

    status: JobState
    dataset_dir: pathlib.Path
    config: str
    cmssw_dir: pathlib.Path
    max_files: int
    max_events: int
    jobs: dict[int, Job]
    output_url: str
    backup_url: str

    def __init__(
        self,
        dataset_dir: pathlib.Path,
        config: str,
        max_files: int = 20,
        max_events: int = -1,
        output_url: str = "",
        backup_url: str = "",
    ) -> None:
        self.dataset_dir = dataset_dir
        self.config = config
        self.cmssw_dir = pathlib.Path()

        self.max_files = max_files
        self.max_events = max_events
        self.output_url = output_url
        self.backup_url = backup_url
        self.jobs = {}
        self.state = JobState.IDLE

        log.debug("Created %s", self)

    def __str__(self) -> str:
        if len(self.jobs):
            return (
                f"Task {self.dataset_dir.name} in state {self.state.name}"
                f" with {len(self.jobs)} jobs."
            )
        else:
            return f"Task {self.dataset_dir.name} in state {self.state.name}"

    @fsm.transition(JobState.IDLE, JobState.READY, on_error=JobState.ERROR)
    def prepare(self, force: bool = False) -> None:

        try:
            input_dir = self.dataset_dir / "input"
            if input_dir.exists():
                if force:
                    shutil.rmtree(input_dir)
                else:
                    raise RuntimeError(f"Directory {input_dir} already exists.")

            input_dir.mkdir()

            self.prepare_sandbox(input_dir)
            self.prepare_config(input_dir)
            nr_files = self.prepare_fileindex(input_dir)
            self.prepare_jobscript(input_dir)

            n = int(nr_files / self.max_files)
            k, m = divmod(nr_files, n)

            log.debug("Creating %d jobs", n)
            self.jobs = {
                i: Job(i, i * k + min(i, m), (i + 1) * k + min(i + 1, m)) for i in range(n)
            }

            log.debug("Prepared %s", self)
        except Exception as e:
            log.error("%s", e)
            raise e

    def prepare_sandbox(self, input_dir: pathlib.Path) -> None:
        """Identify CMSSSW release from installation."""
        try:
            self.cmssw_dir = next(self.dataset_dir.parent.glob("CMSSW_*"))
        except StopIteration:
            raise RuntimeError("No CMSSSW area found.") from None

        log.debug("CMSSW release %s", self.cmssw_dir.name)

        sandbox = input_dir / "sandbox.tar.bz2"
        log.info("Writing %s", sandbox)
        tarball = tarfile.open(sandbox, "w|bz2")

        for dir in SANDBOX_DIRS:
            path = self.cmssw_dir / dir
            if path.exists():
                tarball.add(path)

        for root, _filenames, dirnames in os.walk(self.cmssw_dir / "src"):
            for name in dirnames:
                if name in SANDBOX_SRC_DIRS:
                    tarball.add(os.path.join(root, name))

        tarball.close()

    def prepare_config(self, input_dir: pathlib.Path) -> None:

        inp_cfg = (
            self.cmssw_dir
            / f"src/NanoAOD/NanoAnalyzer/nanoanalyzer_cfg_{self.config}.py"
        )
        out_cfg = input_dir / f"nanoanalyzer_cfg_{self.config}.py"

        log.info("Writing %s", out_cfg)
        re_source = re.compile(r"\s*process\.source\s*=\s*cms\.Source.*")
        re_end = re.compile(r"\s*\).*")
        re_out = re.compile(r"\s*outFile\s*=\s*cms\.string")
        re_evt = re.compile(
            r"\s*process\.maxEvents\s*=\s*cms\.untracked\.PSet\("
            r"\s*input\s*=\s*cms\.untracked\.int32\((\d+)\)\s*\)\s*"
        )

        with open(inp_cfg, "r") as inp, open(out_cfg, "w") as out:
            flag = True
            for i, line in enumerate(inp.readlines()):
                if flag:
                    if match := re_source.match(line):
                        log.debug("Line %3d: Found input", i)
                        flag = False
                        out.write(FRAGMENT1)
                    elif match := re_out.match(line):
                        log.debug("Line %3d: Found output", i)
                        out.write(FRAGMENT2)
                    elif match := re_evt.match(line):
                        log.debug("Line %3d: Found max events", i)
                        out.write(FRAGMENT3.format(self.max_events))
                    else:
                        out.write(line)
                else:
                    match = re_end.match(line)
                    if match:
                        log.debug("Line %3d: End input", i)
                        flag = True
            log.debug("Line %3d: EOF", i)

    def prepare_fileindex(self, input_dir: pathlib.Path) -> int:

        files = []
        for path in self.dataset_dir.iterdir():
            if path.is_file() and path.name.endswith("file_index.txt"):
                files += open(path).readlines()

        file_index = input_dir / "file_index.txt"
        log.debug("Writing %s with %d files", file_index, len(files))
        with open(file_index, "w") as out:
            out.writelines(files)

        return len(files)

    def prepare_jobscript(self, input_dir: pathlib.Path) -> None:

        template = j2_env.get_template("nanoaod.sh.j2")

        jobscript = input_dir / "nanoaod.sh"
        log.debug("Writing %s", jobscript)
        with open(jobscript, "w") as out:
            out.write(
                template.render(
                    dataset=self.dataset_dir.name,
                    jobscript=f"nanoanalyzer_cfg_{self.config}.py",
                    cmssw=self.cmssw_dir.name,
                    output_url=self.output_url,
                    backup_url=self.backup_url,
                )
            )

    @fsm.transition(JobState.READY, JobState.SUBMITTED, on_error=JobState.ERROR)
    def submit(self, max_jobs: int) -> None:

        try:
            template = j2_env.get_template("nanoaod.job.j2")

            jobs = [j for j in self.jobs.values() if j.status == JobState.READY]
            if max_jobs:
                jobs = jobs[:max_jobs]
            job_file = self.dataset_dir / "input" / "nanoaod.job"
            with open(job_file, "w") as out:
                out.write(
                    template.render(
                        dataset=self.dataset_dir.name,
                        jobscript=f"nanoanalyzer_cfg_{self.config}.py",
                        cmssw=self.cmssw_dir.name,
                        jobs=jobs,
                        log_location=LOG_LOCATION,
                        sites=SITES,
                        singularity=SINGULARITY,
                        metadata=PACKAGE_DIR / "scripts/metadata",
                    )
                )

            if not (condor_submit := shutil.which("condor_submit")):
                raise RuntimeError("condor_submit not found.")
            
            if not (python := shutil.which("python2")):
                raise RuntimeError("python2 not found.")

            output = subprocess.check_output([python, condor_submit, str(job_file)])
            print(output)
            cluster = 100
            for i, j in enumerate(jobs):
                j.submit(cluster, i)
                
        except Exception as e:
            log.error("%s", e)
            raise e


class Manager:

    project_dir: pathlib.Path
    output_url: str
    backup_url: str
    tasks: dict[str, Task]

    def __init__(
        self, project_dir: pathlib.Path, output_url: str, backup_url: str
    ) -> None:

        self.project_dir = project_dir
        self.output_url = output_url
        self.backup_url = backup_url

    def __enter__(self) -> None:

        tasks_file = self.project_dir / "tasks_index.pkl"
        if tasks_file.is_file():
            log.debug("Reading tasks from %s", tasks_file)
            try:
                pathlib.Path(self.project_dir / "tasks_index.lock").symlink_to(
                    tasks_file
                )
            except FileExistsError:
                log.fatal("Lock file exists")
                sys.exit()
            with open(tasks_file, "rb") as inp:
                self.tasks = pickle.load(inp)
        else:
            log.debug("No tasks file found.")
            self.tasks = {}

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        with open(self.project_dir / "tasks_index.pkl", "wb") as out:
            pickle.dump(self.tasks, out)

        lock_file = self.project_dir / "tasks_index.lock"
        if lock_file.exists():
            lock_file.unlink()

    def create(
        self, dataset: str, config: str, max_files: int, max_events: int
    ) -> None:

        if dataset in self.tasks:
            log.error("Dataset %s already defined.")
            return

        self.tasks[dataset] = Task(
            self.project_dir / dataset,
            config,
            max_files,
            max_events,
            self.output_url,
            self.backup_url,
        )

    def prepare(self, dataset: str, force: bool = False) -> None:

        self.tasks[dataset].prepare(force)

    def submit(self, dataset: str, max_jobs: int) -> None:

        self.tasks[dataset].submit(max_jobs)
