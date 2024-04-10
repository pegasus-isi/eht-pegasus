#!/usr/bin/env python3

import re
import sys
import argparse
from pathlib import Path

from Pegasus.api import (
    Workflow,
    Container,
    TransformationCatalog,
    ReplicaCatalog,
    SiteCatalog,
    File,
    Job,
    Transformation,
    Namespace,
    OS,
    Arch,
)


class EHTSmili:
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
        # SR1_M87_2017_095_hi_hops_netcal_StokesI
        for f in uvfits_dir.glob("*_[0-9][0-9][0-9]_*.uvfits"):
            self.uvfitsfiles.append(f)

        self.colormap = uvfits_dir.parent / "afmhot_10us.cmap"

        self.d = ("095", "096", "100", "101")

        self.wf = Workflow("eht-smili")
        self.sc = SiteCatalog()
        self.tc = TransformationCatalog()
        self.rc = ReplicaCatalog()

        self.wf.add_site_catalog(self.sc)
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)

    def _load_sc(self): ...

    def _load_tc(self):
        image = "smili"
        container = Container(
            image,
            Container.DOCKER,
            image=f"docker://pegasus/reproducibility-eht:{image}",
            image_site="local",
        ).add_env(
            "PATH",
            "/root/.pyenv/versions/anaconda3-5.3.1/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        )
        self.tc.add_containers(container)

    def _load_rc(self):
        for f in self.uvfitsfiles:
            self.rc.add_replica("local", f.name, f.resolve())

        self.rc.add_replica("local", self.colormap.name, self.colormap.resolve())

    def _generate(self):
        worker = Transformation(
            name="worker",
            namespace="pegasus",
            site="local",
            pfn="https://download.pegasus.isi.edu/pegasus/5.1.0dev/pegasus-worker-5.1.0dev-x86_64_rhel_8.tar.gz",
            is_stageable=True,
            arch=Arch.AARCH64,
        )
        self.tc.add_transformations(worker)

        process = Transformation(
            "smili_imaging_pipeline",
            site="condorpool",
            pfn=self.scripts_dir / "smili_imaging_pipeline.py",
            is_stageable=True,
            arch=Arch.AARCH64,
            container="smili",
        )
        post_process = Transformation(
            "smili_postprocessing",
            site="condorpool",
            pfn=self.scripts_dir / "smili_postprocessing.py",
            is_stageable=True,
            arch=Arch.AARCH64,
            container="smili",
        )

        self.tc.add_transformations(process, post_process)

        pattern = re.compile(r".*_([0-9]{3})_.*")
        for f in self.uvfitsfiles:
            obsdate = pattern.match(f.name).group(1)
            obsdate = int(obsdate) - 90

            job = (
                Job(process)
                .add_args("-i", f.name, "--day", obsdate, "--nproc", "1")
                .add_inputs(f.name)
                .add_outputs(
                    f"{f.stem}.fits",
                    f"{f.stem}.precal.uvfits",
                    f"{f.stem}.selfcal.uvfits",
                )
            )

            self.wf.add_jobs(job)

        for d in self.d:
            post = (
                Job(post_process)
                .add_args(
                    "-i",
                    f"SR1_M87_2017_{d}_hi_hops_netcal_StokesI.fits",
                    "-o",
                    f"SR1_M87_2017_{d}_processed.pdf",
                    "--all",
                )
                .add_inputs(
                    f"SR1_M87_2017_{d}_hi_hops_netcal_StokesI.fits", self.colormap.name
                )
                .add_outputs(f"SR1_M87_2017_{d}_processed.pdf")
            )

            self.wf.add_jobs(post)

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

    eht = EHTSmili(**vars(args))
    eht()


if __name__ == "__main__":
    main()
