# EHT Pegasus Workflow

## Dependencies

1. Install [HTCondor](https://htcondor.readthedocs.io/en/latest/getting-htcondor/)
1. Install [Pegasus WMS](https://pegasus.isi.edu/documentation/user-guide/installation.html)

```sh
pip3 install -r requirements.txt
```

## Run DIFMAP

```sh
cd difmap
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags/${USER}/pegasus/eht-difmap/runXXXX

# Outputs
ls -ltrh dags/output
```

## Run EHT Imaging

```sh
cd eht-imaging
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags/${USER}/pegasus/eht-imaging/runXXXX

# Outputs
ls -ltrh dags/output
```

## Run SMILI

```sh
cd smili
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags/${USER}/pegasus/eht-smili/runXXXX

# Outputs
ls -ltrh dags/output
```

## If running on an ARM based Mac

1. The `smili` workflow fails with an `Illegal Instruction` error. To fix this disable `Use Rosetta for x86_64/amd64 emulation on Apple Silicon` checkbox in Docker settings.
1. The `difmap` workflow might fail if the `Use Rosetta for x86_64/amd64 emulation on Apple Silicon` checkbox is disabled Docker settings.

## Related Publications

Ria Patel, Brandan Roachell, Silvina Caıno-Lores, Charles Ketron, Jacob Leonard,
Nigel Tan, Karan Vahi, Duncan Brown, Ewa Deelman, and Michela Taufer. Reproducibility
of the First Image of a Black Hole in the Galaxy M87 from the Event Horizon Telescope
(EHT) Collaboration. IEEE Computing in Science and Engineering (CiSE), 5(24):42–52, 2022.
https://ieeexplore.ieee.org/document/10040660.

Ross Ketron, Jacob Leonard, Brandan Roachell, Ria Patel, Rebecca White, Silvina Caíno-
Lores, Nigel Tan, Patrick Miles, Karan Vahi, Ewa Deelman, Duncan A. Brown, and Michela
Taufer. A Case Study in Scientific Reproducibility from the Event Horizon Telescope (EHT).
In Proceedings of the 20th IEEE International Conference on eScience, pages 1–2, Innsbruck,
Austria, September 2021. IEEE Computer Society. (Short paper).
http://dx.doi.org/10.1109/eScience51609.2021.00045.

## Acknowledgments

This study was funded by the NSF’s Award Abstract #2331152 Collaborative Research: SHF: Small: Model-driven Design and Optimization of Dataflows for Scientific Applications.

## Contact Information

Please contact Drs. Ewa Deelman (deelman@isi.edu) or Michela Taufer (taufer@utk.edu) for information on the workflow and the reproducibility of the results.