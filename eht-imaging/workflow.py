#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path

from Pegasus.api import (
    Arch,
    Container,
    Directory,
    FileServer,
    Job,
    Operation,
    OS,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)


class EHTImaging:
    def __init__(self, uvfits_dir, scripts_dir):
        if not uvfits_dir:
            raise ValueError("uvfitsdir is required")

        if not scripts_dir:
            raise ValueError("scripts_dir is required")

        self.uvfits_dir = uvfits_dir = Path(uvfits_dir).resolve()
        self.scripts_dir = scripts_dir = Path(scripts_dir).resolve()

        if not uvfits_dir.is_dir():
            raise ValueError("uvfitsdir must be a directory")

        if not scripts_dir.is_dir():
            raise ValueError("scripts_dir must be a directory")

        self.uvfitsfiles = []
        for f in uvfits_dir.glob("*.uvfits"):
            self.uvfitsfiles.append(f)

        self.eht_difmap = uvfits_dir.parent / "EHT_Difmap"
        self.mask = uvfits_dir.parent / "CircMask_r30_x-0.002_y0.022.win"
        self.colormap = uvfits_dir.parent / "afmhot_10us.cmap"

        self.d = ("095", "096", "100", "101")

        self.wf = Workflow("eht-imaging")
        self.sc = SiteCatalog()
        self.tc = TransformationCatalog()
        self.rc = ReplicaCatalog()

        self.wf.add_site_catalog(self.sc)
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)

    def _load_sc(self):
        # create a "local" site
        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
            Directory(
                Directory.SHARED_STORAGE,
                path=(Path.cwd() / "dags" / "wf-output").resolve(),
            ).add_file_servers(
                FileServer((Path.cwd() / "dags" / "wf-output").as_uri(), Operation.ALL)
            ),
            Directory(
                Directory.SHARED_SCRATCH,
                path=(Path.cwd() / "dags" / "wf-scratch" / "LOCAL").resolve(),
            ).add_file_servers(
                FileServer(
                    (Path.cwd() / "dags" / "wf-scratch" / "LOCAL").as_uri(),
                    Operation.ALL,
                )
            ),
        )

        condorpool = Site(
            "condorpool", arch=Arch.X86_64, os_type=OS.LINUX
        ).add_pegasus_profile(style="condor")

        self.sc.add_sites(local, condorpool)

    def _load_tc(self):
        image = "eht-imaging"
        container = (
            Container(
                image,
                Container.DOCKER,
                image=f"docker://pegasus/reproducibility-eht:{image}",
                image_site="local",
            )
            .add_env("PYTHONPATH", "/home/eht/.local/lib/python3.8/site-packages")
            .add_env(
                key="PATH",
                value="/opt/conda/envs/eht-imaging/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
        )
        self.tc.add_containers(container)

    def _load_rc(self):
        for f in self.uvfitsfiles:
            self.rc.add_replica("local", f.name, f.resolve())

        self.rc.add_replica("local", self.colormap.name, self.colormap.resolve())

    def _generate(self):
        process = Transformation(
            "eht-imaging_pipeline",
            site="condorpool",
            pfn=self.scripts_dir / "eht-imaging_pipeline.py",
            is_stageable=True,
            container="eht-imaging",
        )
        post_process = Transformation(
            "eht-imaging_postprocessing",
            site="condorpool",
            pfn=self.scripts_dir / "eht-imaging_postprocessing.py",
            is_stageable=True,
            container="eht-imaging",
        )

        self.tc.add_transformations(process, post_process)

        for d in self.d:
            job = (
                Job(process)
                .add_args(
                    "-i",
                    f"SR1_M87_2017_{d}_lo_hops_netcal_StokesI.uvfits",
                    "-i2",
                    f"SR1_M87_2017_{d}_hi_hops_netcal_StokesI.uvfits",
                    "-o",
                    f"SR1_M87_2017_{d}.fits",
                    "--savepdf",
                    "--imgsum",
                )
                .add_inputs(
                    f"SR1_M87_2017_{d}_lo_hops_netcal_StokesI.uvfits",
                    f"SR1_M87_2017_{d}_hi_hops_netcal_StokesI.uvfits",
                )
                .add_outputs(
                    f"SR1_M87_2017_{d}.fits",
                    f"SR1_M87_2017_{d}.pdf",
                    f"SR1_M87_2017_{d}_imgsum.pdf",
                    register_replica=False,
                )
            )
            post = (
                Job(post_process)
                .add_args(
                    "-i",
                    f"SR1_M87_2017_{d}.fits",
                    "-o",
                    f"SR1_M87_2017_{d}_processed.pdf",
                    "--blur",
                    "--afmhot10us",
                    "--notitle",
                )
                .add_inputs(f"SR1_M87_2017_{d}.fits", self.colormap.name)
                .add_outputs(f"SR1_M87_2017_{d}_processed.pdf", register_replica=False)
            )

            self.wf.add_jobs(job, post)

    def __call__(self):
        self._load_sc()
        self._load_tc()
        self._load_rc()

        self._generate()

        self.wf.write(sys.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u",
        "--uvfits-dir",
        required=True,
        metavar="input uvfits directory",
        type=str,
        help="input uvfits directory of the data product release 2019-XX-XXXX",
    )
    parser.add_argument(
        "-s",
        "--scripts-dir",
        required=True,
        metavar="Scripts directory",
        type=str,
        help="Scripts directory",
    )
    args = parser.parse_args()

    eht = EHTImaging(**vars(args))
    eht()


if __name__ == "__main__":
    main()
