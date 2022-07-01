#!/usr/bin/env python

import subprocess

import ruamel.yaml

XRDADLER32 = "/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.5/3.5.62-1/el7-x86_64/usr/bin/xrdadler32"


def main():

    yaml = ruamel.yaml.YAML(typ="safe")
    with open("metadata.yaml", "r") as inp:
        data = yaml.load(inp)
    for file in data["files"]:
        url = file["url"]
        chksum = file["checksum"]
        # if chksum[0] == "x":
        #     chksum = "%8.8x" % (int("-0" + chksum, 16) & 0xFFFFFFFF)

        output = subprocess.run(
            [XRDADLER32, file["url"]],
            check=True,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
        ).stdout
        chksum1 = output.split()[0]
        if chksum == chksum1:
            print(url, "OK")
        else:
            print(url, "Bad")
