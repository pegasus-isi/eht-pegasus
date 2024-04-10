#!/usr/bin/env python3

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


class EHTDIFMAP:
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

        self.wf = Workflow("eht-difmap")
        self.sc = SiteCatalog()
        self.tc = TransformationCatalog()
        self.rc = ReplicaCatalog()

        self.wf.add_site_catalog(self.sc)
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)

    def _load_sc(self): ...

    def _load_tc(self):
        image = "difmap"
        container = (
            Container(
                image,
                Container.DOCKER,
                image=f"docker://globalcomputinglab/reproducibility-eht:{image}",
                image_site="local",
            )
            .add_env("PYTHONPATH", "/home/eht/.local/lib/python3.8/site-packages")
            .add_env(
                key="PATH",
                value="/opt/conda/envs/eht-imaging/bin:/opt/conda/condabin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/pgplot:/home/eht/uvf_difmap_2.5k",
            )
        )
        self.tc.add_containers(container)

    def _load_rc(self):
        for f in self.uvfitsfiles:
            self.rc.add_replica("local", f.name, f.resolve())

        self.rc.add_replica("local", self.eht_difmap.name, self.eht_difmap.resolve())
        self.rc.add_replica("local", self.mask.name, self.mask.resolve())
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

        difmap = Transformation(
            "difmapp",
            site="condorpool",
            pfn=self.scripts_dir / "difmap.sh",
            is_stageable=True,
            arch=Arch.AARCH64,
            container="difmap",
        )
        difmap_post = Transformation(
            "difmap-postprocessing",
            site="condorpool",
            pfn=self.scripts_dir / "difmap-postprocessing.py",
            is_stageable=True,
            arch=Arch.AARCH64,
            container="difmap",
        )
        difmap_imgsum = Transformation(
            "difmap-imgsum",
            site="condorpool",
            pfn=self.scripts_dir / "difmap-imgsum.py",
            is_stageable=True,
            arch=Arch.AARCH64,
            container="difmap",
        )
        self.tc.add_transformations(difmap, difmap_post, difmap_imgsum)

        for f in self.uvfitsfiles:
            suffix = "RT-10.CF0.5.ALMA0.1.UVW2_-1"
            job = (
                Job(difmap)
                .add_args(f.name)
                .add_inputs(f.name, self.mask.name, self.eht_difmap.name)
                .add_outputs(
                    *(
                        f"{f.stem}.{self.mask.stem}.{suffix}.fits",
                        f"{f.stem}.{self.mask.stem}.{suffix}.mod",
                        f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.fits",
                        f"{f.stem}.{self.mask.stem}.{suffix}.par",
                        f"{f.stem}.{self.mask.stem}.{suffix}.stat",
                        f"{f.stem}.{self.mask.stem}.{suffix}.uvf",
                        f"{f.stem}.{self.mask.stem}.{suffix}.win",
                    )
                )
            )

            job_post_1 = (
                Job(difmap_post)
                .add_args(
                    "-i",
                    f"{f.stem}.{self.mask.stem}.{suffix}.fits",
                    "-o",
                    f"{f.stem}.{self.mask.stem}.{suffix}.pdf",
                    "--all",
                )
                .add_inputs(
                    self.colormap.name,
                    f"{f.stem}.{self.mask.stem}.{suffix}.fits",
                )
                .add_outputs(f"{f.stem}.{self.mask.stem}.{suffix}.pdf")
            )
            job_post_2 = (
                Job(difmap_post)
                .add_args(
                    "-i",
                    f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.fits",
                    "-o",
                    f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.pdf",
                    "--all",
                )
                .add_inputs(
                    self.colormap.name,
                    f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.fits",
                )
                .add_outputs(f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.pdf")
            )

            self.wf.add_jobs(job, job_post_1, job_post_2)

            if "_hi_" in f.stem:
                continue

            job_imgsum = (
                Job(difmap_imgsum)
                .add_args(
                    "-i",
                    f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.fits",
                    "-o",
                    f.name,
                    "-O",
                    ".",
                )
                .add_inputs(
                    self.colormap.name,
                    f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.fits",
                    f.name,
                )
                .add_outputs(f"{f.stem}.{self.mask.stem}.{suffix}.noresiduals.img.pdf")
            )

            self.wf.add_jobs(job_imgsum)

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

    eht = EHTDIFMAP(**vars(args))
    eht()


if __name__ == "__main__":
    main()
