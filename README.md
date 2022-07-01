## Setup NanoAODRun1 for 2011/2012

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

```bash
```

