#!/usr/bin/env python

import subprocess
import json
import csv

import click

PERIODES = ["2010", "2011", "2012", "2015"]

DASGOCLIENT = "/cvmfs/cms.cern.ch/common/dasgoclient"


@click.command
@click.option("--period", default="2011", type=click.Choice(PERIODES))
def main(period: list[str]):
    """Find Opendata datasets from DAS"""

    output = subprocess.run(
        [DASGOCLIENT, "-json", f"-query=dataset dataset=/*/Run{period}*/AOD"],
        check=True,
        stdout=subprocess.PIPE,
        encoding="UTF-8",
    ).stdout
    dsets_info = json.loads(output)

    datasets = []
    for d in dsets_info:
        for d1 in d["dataset"]:
            name = d1["name"]
            primary = d1["primary_ds_name"]
            output = subprocess.run(
                [DASGOCLIENT, "-json", f"-query=site dataset={name}"],
                check=True,
                stdout=subprocess.PIPE,
                encoding="UTF-8",
            ).stdout
            site_info = json.loads(output)
            sites = []
            for s in site_info:
                sites += [s1["name"] for s1 in s["site"]]
            print(f"{name:<60} - {','.join(sites)}")
            if "T3_CH_CERN_OpenData" not in sites:
                continue
            output = subprocess.run(
                [DASGOCLIENT, "-json", f"-query=file dataset={name}"],
                check=True,
                stdout=subprocess.PIPE,
                encoding="UTF-8",
            ).stdout
            file_info = json.loads(output)
            size = 0
            nr_files = 0
            for f in file_info:
                nr_files += len(f)
                size += sum(f1["size"] for f1 in f["file"])
            datasets.append(
                {
                    "Dataset": primary,
                    "Name": name,
                    "#Files": nr_files,
                    "Size": size / (1024 * 1024 * 1024),
                }
            )

    for d in sorted(datasets, key=lambda x: f"{x['Dataset']} {x['Name']}"):
        print(f"{d['Dataset']:<20} - {d['Name']:<60} - {d['Size']:8.2f} GB")

    with open(f"OpenData_Run{period}.csv", "w") as out:
        dsw = csv.DictWriter(out, fieldnames=["Dataset", "Name", "#Files", "Size"])
        dsw.writeheader()
        dsw.writerows(sorted(datasets, key=lambda x: f"{x['Dataset']} {x['Name']}"))


if __name__ == "__main__":
    main()
