#!/usr/bin/env python

import json
import fnmatch
import os

import ruamel.yaml


def main():

    metadata = []
    for name in os.listdir():
        if fnmatch.fnmatch(name, "metadata_*.json"):
            with open(name, "r") as inp:
                metadata.append(json.load(inp))

    # fix checksum hex

    for m in metadata:
        chksum = m['checksum']
        if chksum[0] == "x":
            m['checksum'] = "%8.8x" % (int("-0" + chksum, 16) & 0xFFFFFFFF)

    with open("metadata.yaml", "w") as out:
        yaml = ruamel.yaml.YAML()
        yaml.explicit_start = True
        yaml.indent(sequence=4, offset=2)
        yaml.dump(
            {
                "dataset": "/SingleMu/Run2011B-12Oct2013-v1-XXXXX/NANOAOD",
                "doi" : "XX.XXXXX/OPENDATA.CMS.XBTD.NKD3",
                "parent": "/SingleMu/Run2011B-12Oct2013-v1/AOD",
                "parent_doi": "10.7483/OPENDATA.CMS.XBTD.NKD3",
                "cmssw" : "CMSSW_5_4_11",
                "nanoanalyser": "XXX",
                "files": metadata,
            },
            out,
        )
