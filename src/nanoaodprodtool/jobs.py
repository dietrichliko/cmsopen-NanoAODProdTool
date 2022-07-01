"""A job implemented as FSM."""
import collections
import enum
import json
import logging
import os
import pathlib
import pickle
import re
import shutil
import subprocess
import sys
import tarfile
from types import TracebackType
from typing import Any
from typing import Optional

import finite_state_machine as fsm
import jinja2
import ruamel.yaml

from nanoaodprodtool.test import XRDADLER32

log = logging.getLogger(__package__)
j2_env = jinja2.Environment(loader=jinja2.PackageLoader(__package__, "templates"))

SANDBOX_DIRS = ["bin", "cfipython", "config", "lib", "module", "python"]
SANDBOX_SRC_DIRS = ["data", "interface", "python", "files"]

FRAGMENT1 = """
import os
files = open("file_index.txt", "r").read().splitlines()
f = int( os.environ["FIRST_FILE"])
l = int( os.environ["LAST_FILE"])
process.source = cms.Source("PoolSource",
  fileNames = cms.untracked.vstring(*files[f:l])
)
"""

FRAGMENT2 = """
                outFile = cms.string('output.root'),
"""

FRAGMENT3 = """
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({}))
"""

FRAGMENT4 = """
goodJSON = '{}/src/NanoAOD/NanoAnalyzer/files/{}'
"""

LOG_LOCATION = f"/scratch/{os.getlogin()}/nanojobs"

PACKAGE_DIR = pathlib.Path(__file__).parent


def which(name: str) -> str:

    path = shutil.which(name)
    if path is None:
        log.error("Binary %s not found.")
        sys.exit()

    return path


PYTHON2 = which("python2")
CONDOR_SUBMIT = which("condor_submit")
CONDOR_Q = which("condor_q")
DASGOCLIENT = which("dasgoclient")
VOMS_PROXY_INFO = which("voms-proxy-info")
VOMS_PROXY_INIT = which("voms-proxy-init")


class CondorStatus(enum.IntEnum):
    """Status of a HTCodor job"""

    Unexpanded = 0
    Idle = 1
    Running = 2
    Removed = 3
    Completed = 4
    Held = 5
    Submission_err = 6


class JobState(enum.Enum):
    """Job states."""

    IDLE = 1
    READY = 2
    RUNNING = 3
    DONE = 4
    ERROR = 5


class Job(fsm.StateMachine):
    """A NanoAODPlus Job.

    States:
        IDLE - Job created
        READY - Job submitted to HTCondor
        RUNNING - Job running on HTCondor
        DONE - Job excution finished
        ERROR - Job failed
    """

    task: "Task"
    state: JobState
    id: int
    first_file: int
    last_file: int
    cluster_id: Optional[int]
    process_id: Optional[int]
    condor_state: Optional[CondorStatus]

    def __init__(self, task: "Task", id: int, first_file: int, last_file: int) -> None:
        """Create job.

        Args:
            task (Task): Job belongs to this Task
            id (int): Job number in task
            first_file (int): First file to process
            last_file (int): Last file to process
        """
        log.debug("Creating job %s - %d", task.dataset_dir.name, id)
        self.task = task
        self.state = JobState.IDLE
        self.id = id
        self.first_file = first_file
        self.last_file = last_file

        self.condor_state = None
        self.cluster_id = None
        self.process_id = None

    def __str__(self) -> str:
        txt = f"Job {self.id:4d} - {self.state.name:<7s}"
        if self.condor_state is not None:
            txt += (
                f"{self.cluster_id:9d} - {self.process_id:3d}"
                f" - {self.condor_state.name}"
            )
        return txt

    @fsm.transition([JobState.IDLE, JobState.ERROR], JobState.READY)
    def submit(self, cluster: int, process: int) -> None:
        log.debug("Submitting job %s - %d", self.task.dataset_dir.name, self.id)
        self.cluster_id = cluster
        self.process_id = process

    @fsm.transition(JobState.READY, JobState.RUNNING)
    def running(self) -> None:
        log.debug("Running job %s - %d", self.task.dataset_dir.name, self.id)

    @fsm.transition(
        [JobState.READY, JobState.RUNNING],
        JobState.DONE,
        on_error=JobState.ERROR,
    )
    def finish(self) -> None:
        log.debug("Finishing job %s - %d", self.task.dataset_dir.name, self.id)
        self.condor_state = None
        self.cluster_id = None
        self.process_id = None

        try:
            with open(self.task.dataset_dir / f"output/metadata_{self.id}.json") as inp:
                metadata = json.load(inp)

            output = subprocess.run(
                [XRDADLER32, metadata["url"]],
                check=True,
                stdout=subprocess.PIPE,
                encoding="UTF-8",
            ).stdout
            if output.split()[0] != metadata["checksum"]:
                log.error("Checksum mismatch for %s", metadata["url"])
                raise RuntimeError("Checksum mismatch")

        except Exception as e:
            log.error(e)
            raise e

    def update(self, condor_state: CondorStatus) -> None:

        self.condor_state = condor_state
        if self.state == JobState.READY:
            if condor_state == CondorStatus.Idle:
                pass
            elif condor_state == CondorStatus.Running:
                self.running()
            elif condor_state == CondorStatus.Completed:
                self.finish()
            else:
                log.error(
                    "Unexpected condor state %s for job %s - %d",
                    condor_state.name,
                    self.task.dataset_dir.name,
                    self.id,
                )


class Task(fsm.StateMachine):
    """A NanoAOD Task.

    States:
        IDLE - Task created
        READY - Task prapared
        RUNNING - Task running on HTCondor
        DONE - Task excution finished
        ERROR - Task failed
    """

    state: JobState
    dataset_dir: pathlib.Path
    dasname: str
    config: str
    cmssw_dir: pathlib.Path
    max_files: int
    max_events: int
    jobs: dict[int, Job]
    output_url: str
    backup_url: str
    singularity: str
    sites: list[str]

    def __init__(
        self,
        dataset_dir: pathlib.Path,
        dasname: str,
        config: str,
        output_url: str,
        backup_url: str,
        singularity: str,
        sites: list[str],
        max_files: int = 20,
        max_events: int = -1,
    ) -> None:
        self.dataset_dir = dataset_dir
        self.dasname = dasname
        self.config = config
        self.cmssw_dir = pathlib.Path()

        self.output_url = output_url
        self.backup_url = backup_url
        self.singularity = singularity
        self.sites = sites

        self.max_files = max_files
        self.max_events = max_events

        self.jobs = {}
        self.state = JobState.IDLE

        log.info("Created Task %s", self)

    def states(self) -> dict[JobState, int]:

        states: dict[JobState, int] = collections.defaultdict(int)
        for j in self.jobs.values():
            states[j.state] += 1

        return states

    def __str__(self) -> str:
        txt = f"Task {self.dataset_dir.name:<40s} - {self.state.name:<7s}"
        if len(self.jobs):
            txt += f" - {', '.join(f'{s.name}: {i}' for s, i in self.states().items())}"

        return txt

    @fsm.transition(JobState.IDLE, JobState.READY, on_error=JobState.ERROR)
    def prepare(self, force: bool = False) -> None:

        try:
            input_dir = self.dataset_dir / "input"
            output_dir = self.dataset_dir / "output"
            if input_dir.exists():
                if force:
                    shutil.rmtree(input_dir)
                else:
                    raise RuntimeError(f"Directory {input_dir} already exists.")

            if output_dir.exists():
                if force:
                    shutil.rmtree(output_dir)
                else:
                    raise RuntimeError(f"Directory {output_dir} already exists.")

            input_dir.mkdir()
            output_dir.mkdir()

            self.prepare_sandbox(input_dir)
            self.prepare_config(input_dir)
            nr_files = self.prepare_fileindex(input_dir)
            self.prepare_jobscript(input_dir)

            n = int(nr_files / self.max_files)
            k, m = divmod(nr_files, n)

            log.debug("Creating %d jobs", n)
            self.jobs = {
                i: Job(self, i, i * k + min(i, m), (i + 1) * k + min(i + 1, m))
                for i in range(n)
            }

            log.info("Prepared %s", self)

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

        sandbox = input_dir / "sandbox.tar.gz"
        log.debug("Writing %s", sandbox)
        tarball = tarfile.open(sandbox, "w:gz")

        for dir in SANDBOX_DIRS:
            path = self.cmssw_dir / dir
            arc_path = path.relative_to(self.cmssw_dir.parent)
            if path.exists():
                tarball.add(path, arc_path)

        for root, dirnames, _filenames in os.walk(self.cmssw_dir / "src"):
            root_path = pathlib.Path(root)
            for name in dirnames:
                if name in SANDBOX_SRC_DIRS:
                    path = root_path / name
                    arc_path = path.relative_to(self.cmssw_dir.parent)
                    tarball.add(path, arc_path)

        tarball.close()

    def prepare_config(self, input_dir: pathlib.Path) -> None:

        inp_cfg = (
            self.cmssw_dir
            / f"src/NanoAOD/NanoAnalyzer/nanoanalyzer_cfg_{self.config}.py"
        )
        out_cfg = input_dir / f"nanoanalyzer_cfg_{self.config}.py"

        log.debug("Writing %s", out_cfg)
        re_source = re.compile(r"\s*process\.source\s*=\s*cms\.Source.*")
        re_end = re.compile(r"\s*\).*")
        re_out = re.compile(r"\s*outFile\s*=\s*cms\.string")
        re_evt = re.compile(
            r"\s*process\.maxEvents\s*=\s*cms\.untracked\.PSet\("
            r"\s*input\s*=\s*cms\.untracked\.int32\((\d+)\)\s*\)\s*"
        )
        re_json = re.compile(r"\s*goodJSON\s*=\s*'files/([^']+)'.*")

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
                    elif match := re_json.match(line):
                        log.debug("Line %3d: Found json", i)
                        out.write(FRAGMENT4.format(self.cmssw_dir.name, match.group(1)))
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

    def submit(self, max_jobs: int, resubmit: bool = False) -> None:

        pathlib.Path(LOG_LOCATION).mkdir(exist_ok=True)
        try:
            template = j2_env.get_template("nanoaod.job.j2")

            if resubmit:
                jobs = [
                    j
                    for j in self.jobs.values()
                    if j.state in [JobState.IDLE, JobState.ERROR]
                ]
            else:
                jobs = [j for j in self.jobs.values() if j.state == JobState.IDLE]
            if not jobs:
                return
            if max_jobs:
                jobs = jobs[:max_jobs]
            job_file = self.dataset_dir / "input/nanoaod.job"
            with open(job_file, "w") as out:
                out.write(
                    template.render(
                        dataset=self.dataset_dir.name,
                        jobscript=self.dataset_dir
                        / f"input/nanoanalyzer_cfg_{self.config}.py",
                        cmssw=self.cmssw_dir.name,
                        jobs=jobs,
                        log_location=LOG_LOCATION,
                        sites=self.sites,
                        singularity=self.singularity,
                        metadata=PACKAGE_DIR / "scripts/metadata",
                    )
                )

            output = subprocess.run(
                [PYTHON2, CONDOR_SUBMIT, str(job_file)],
                check=True,
                stdout=subprocess.PIPE,
                encoding="UTF-8",
            ).stdout

            match = re.search(r"cluster\s+(\d+)\.", output)
            if match:
                cluster = int(match.group(1))
                log.info(
                    "Submitting %s jobs from %d to HTCondor %d",
                    self.dataset_dir.name,
                    len(jobs),
                    cluster,
                )
                for i, j in enumerate(jobs):
                    j.submit(cluster, i)
            else:
                log.error("Could not match cluster id %s.", output)
                raise RuntimeError("Could not match clusterid.")

        except Exception as e:
            log.error("%s", e)
            raise e

    @fsm.transition([JobState.READY, JobState.RUNNING], JobState.DONE)
    def finish(self) -> None:

        file_metadata: list[dict[str, Any]] = []
        for job in self.jobs.values():
            with open(self.dataset_dir / f"output/metadata_{job.id}.json", "r") as inp:
                file_metadata.append(json.load(inp))

        metadata = {
            "dataset": self.dataset_dir.name,
            "parent": self.dasname,
            "files": file_metadata,
        }
        with open(self.dataset_dir / "output/metadata.yaml", "w") as out:
            yaml = ruamel.yaml.YAML()
            yaml.indent(sequence=4, offset=2)
            yaml.dump(metadata, out)

    def update(self) -> None:

        log.debug("Updateing %s", self.dataset_dir.name)
        cluster_ids = set(j.cluster_id for j in self.jobs.values() if j.cluster_id)

        if not cluster_ids:
            return

        output = subprocess.run(
            [CONDOR_Q, "-json"] + [str(id) for id in cluster_ids],
            check=True,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
        ).stdout
        if output:
            job_states = json.loads(output)
            for state in job_states:
                job_nr = int(state["Args"].split()[0])
                status = CondorStatus(state["JobStatus"])
                self.jobs[job_nr].update(status)
        else:
            for job in self.jobs.values():
                if (
                    job.state in [JobState.READY, JobState.RUNNING]
                    and job.cluster_id in cluster_ids
                ):
                    job.finish()

        if any(j.state == JobState.RUNNING for j in self.jobs.values()):
            self.state = JobState.RUNNING

        if any(j.state == JobState.ERROR for j in self.jobs.values()):
            self.state = JobState.ERROR

        if all(j.state == JobState.DONE for j in self.jobs.values()):
            self.finish()


class Manager:

    project_dir: pathlib.Path
    output_url: str
    backup_url: str
    singularity: str
    sites: list[str]
    tasks: dict[str, Task]

    def __init__(
        self,
        project_dir: pathlib.Path,
        output_url: str,
        backup_url: str,
        singularity: str,
        sites: list[str],
    ) -> None:

        self.project_dir = project_dir
        self.output_url = output_url
        self.backup_url = backup_url
        self.singularity = singularity
        self.sites = sites
        self.tasks = {}

    def __enter__(self) -> None:

        check_voms_proxy()

        if not self.tasks:
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
                log.debug("New task file.")

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
        self,
        dataset: str,
        dasname: str,
        config: str,
        max_files: int,
        max_events: int,
        force: bool,
    ) -> Optional[Task]:

        if dataset in self.tasks and not force:
            log.error("Dataset %s already defined.", dataset)
            return None

        dataset_dir = self.project_dir / dataset

        if dataset_dir.exists():
            if force:
                shutil.rmtree(dataset_dir)
            else:
                log.error("Dataset directory %s altready exists", dataset_dir)
                return None

        dataset_dir.mkdir()

        # output = subprocess.run(
        #     [DASGOCLIENT, "-json", f"-query=site dataset={dasname}"],
        #     check=True,
        #     stdout=subprocess.PIPE,
        #     encoding="UTF-8",
        # ).stdout

        # site_info = json.loads(output)

        # sites: list[str] = []
        # for si in site_info:
        #     sites += [s["name"] for s in si["site"]]

        # if "T3_CH_CERN_OpenData" not in sites:
        #     log.fatal("Dataset %s not at T3_CH_CERN_OpenData", dasname)
        #     sys.exit(1)

        output = subprocess.run(
            [DASGOCLIENT, "-json", f"-query=file dataset={dasname}"],
            check=True,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
        ).stdout

        files_info = json.loads(output)

        files: list[str] = []
        for fi in files_info:
            files += [f["name"] for f in fi["file"]]

        for i, f in enumerate(files):
            if f.startswith("/store/data"):
                files[i] = f"root://eospublic.cern.ch//eos/opendata/cms/{f[12:]}\n"
            elif f.startswith("/store/mc"):
                if config[:4] in ["2011", "2012"]:
                    files[
                        i
                    ] = f"root://eospublic.cern.ch//eos/opendata/cms/MonteCarlo{config[:4]}/{f[10:]}\n"
                else:
                    files[
                        i
                    ] = f"root://eospublic.cern.ch//eos/opendata/cms/mc/{f[10:]}\n"
            else:
                log.error("Unexpected filename %s", f)

        with open(dataset_dir / "file_index.txt", "w") as out:
            out.writelines(files)

        task = Task(
            self.project_dir / dataset,
            dasname,
            config,
            self.output_url + dataset,
            self.backup_url + dataset,
            self.singularity,
            self.sites,
            max_files,
            max_events,
        )
        self.tasks[dataset] = task
        return task

    def update(self):

        for task in self.tasks.values():
            task.update()

    def states(self) -> dict[JobState, int]:

        states = {s: 0 for s in JobState}
        for task in self.tasks.values():
            for s, i in task.states().items():
                states[s] += i
        return states


def check_voms_proxy() -> None:

    try:
        output = subprocess.run(
            [VOMS_PROXY_INFO, "-timeleft"],
            check=True,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
        ).stdout
        timeleft = int(output)
    except subprocess.SubprocessError:
        timeleft = 0

    # 5 days
    if timeleft < 432000:
        try:
            subprocess.run(
                [VOMS_PROXY_INIT, "-rfc", "-voms", "cms", "-valid", "192:0"],
                check=True,
                stdout=subprocess.PIPE,
                encoding="UTF-8",
            ).stdout
        except subprocess.SubprocessError:
            log.fatal("Error obtaining user proxy.")
            sys.exit()
