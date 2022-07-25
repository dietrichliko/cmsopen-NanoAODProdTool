# NANOJOBS

A simple framework for NanoAODRun1 production based on HTCondor.

It supports the submission of jobs in legacy CMS environments, that have
no support in current analysis tools. 

## Introduction

The CMSCONNECT facility provides access to the CMS grid sites based on HTCondor.
This allows the submission of simple jobs with an arbirary payload to the computing sites.
The sites, which has to be fixed manually, should have a good network connection to CERN for access of data from the CMS opendata area and to the storge area where results were deposited

NANOJOBS is a simple wrapper that automates certain aspects of the creation and submission
of grid jobs based on HTCondor. A simple state machine mechanism allows to organise 
the actions and trigger corresponfding actions on state transtion.

Two nested statemachines are defined on different levels: The *Task statemachine* desctibes
the CMS datasets, which conatins many files (typically of the order of thousand files). It delegates the work underlying *Job statemachine*, correspondig to a splitting of the dataset into subsets with few files each (typically about 50 files). Both implements the same states,
with different operations on the tansitions. Following states are implemented

* IDLE - initial state. The jobs have been prepared and are ready for submission.
* READY - jobs have been submitted to the grid site
* RUNNING - jobs are being executed on the grid site
* DONE - jobs are finished and their results verified
* ERROR - job failers during execution or at output verification

Following functions have been implemented

* Creating a sandbox tar file for the CMSSW
* Job will run in legacy environment defined by Singularity container in cvmfs
* Creating a file index based on CMS DAS information and the opendata server
* Spilting of the task to process a dataset into individual jobs based on 
  a maximum number of files per job
* Manual job throtteling to avoid too many queued jobs
* Submission to a fixed CMS computing site.  
* Storage of results on a defined location on a storage element. In the ideal
  case this storage element should be on the site, where the jobs are executed
* Creating of result metadata (number of entries, adler32 checksum)
* Verification of output results with the checksum

## Installation

The program is a python package using python-poetry and is intended to be run
in a conda environment

```bash
git clone .... NanoAODProdTool
cd NanoAODProdTool
conda create --file=nanojobs-env.yaml
poetry install
```

To use the program activate the conda environment
```bash
conda activate nanojobs
```

## Commands

Following command have been implemented

### create

```
Usage: nanojobs create [OPTIONS] DATASET

  Create task to process a dataset.

Options:
  --dasname TEXT
  --config [2010Data|2010MC|2011Datapp2.76|2011Data|2011MC|2012Data|2012MC|2015Data|2015MC|2015miniMC|2015DataPoet|2015MCPoet]
                                  NanoAODplus configuration  [required]
  --max-files INTEGER RANGE       Max number of files per job  [default: 40;
                                  x>=1]
  --max-events INTEGER            Max number of events per job (default:no
                                  limit)
  --force / --no-force            Overwrite input folder
  --help                          Show this message and exit.
  ```

### mcreate

```
Usage: nanojobs mcreate [OPTIONS] FILE

  Create task from a list of datasets in a file.

Options:
  --max-files INTEGER RANGE  Max number of files per job  [default: 40; x>=1]
  --max-events INTEGER       Max number of events per job (default:no limit)
  --force / --no-force       Overwrite input folder.
  --poet / --no-poet         POET Configuration
  --help                     Show this message and exit.
```

### submit

```
Usage: nanojobs submit [OPTIONS] DATASET

  Submit the jobs of a dataset to the grid.

Options:
  --max-jobs INTEGER RANGE  [x>=0]
  --resubmit
  --help                    Show this message and exit.
```

### msubmit

```
Usage: nanojobs msubmit [OPTIONS]

  Submit multiple task until a maximal number of jobs is reached.

Options:
  --max INTEGER RANGE  [x>=0]
  --help               Show this message and exit.
```

### list

```
Usage: nanojobs list [OPTIONS] [DATASET]

  List task and their prcessing status.

Options:
  --help  Show this message and exit.
```

### metadata

```
Usage: nanojobs metadata [OPTIONS] DATASET

  Write metadata for job in metadata.yaml amd file_index.txt.

Options:
  --url TEXT
  --force
  --help      Show this message and exit.
```

### remove

```
Usage: nanojobs remove [OPTIONS] DATASET

  Remove a task from the DB.

Options:
  --cleanup
  --help     Show this message and exit.
```

## Setup differnt configuration

### Setup NanoAODRun1 for 2011/2012

Create new folder
```bash
# Create binaries inside the singularity container
cmssw-cc6
cmsrel CMSSW_5_3_32
cd CMSSW_5_3_32/src
cmsenv
kinit -fp <cernid>@CERN.CH
git clone https://:@gitlab.cern.ch:8443/cms-opendata/cms-opendata-nanoaodplus/nanoaodplus_v1.git NanoAOD/NanoAnalyzer
scram -b -j4
```

Configure singularity in ```nanojobs.ini```
```
singularity = /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/slc6:x86_64
```


## Setup NanaAODRun1 for 2010

Create new folder
```bash
# Create binaries inside the singularity container
cmssw-env --cms-os slc5
cmsrel CMSSW_4_2_8
cd CMSSW_4_2_8/src
cmsenv
kinit -fp <cernid>@CERN.CH
git clone https://:@gitlab.cern.ch:8443/cms-opendata/cms-opendata-nanoaodplus/nanoaodplus_v1.git NanoAOD/NanoAnalyzer
scram -b -j4
```

Configure singularity in ```nanojobs.ini```
```
singularity = /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/slc5:latest
```

## POET

Yet to be written

