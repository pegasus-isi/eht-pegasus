# EHT Pegasus Workflow

## Dependencies

```sh
pip3 install -r requirements.txt
```

## Run DIFMAP

```sh
cd difmap
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags

# Outputs
ls -ltrh dags/wf-output
```

## Run EHT Imaging

```sh
cd eht-imaging
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags

# Outputs
ls -ltrh dags/wf-output
```

## Run SMILI

```sh
cd smili
./run.sh ../data/uvfits ../scripts

# Monitor workflow
pegasus-status -w -l dags

# Outputs
ls -ltrh dags/wf-output
```

## If running on an ARM based Mac

1. In `pegasus.properties` file set `JAVA_HOME` environment variable. For example, `env.JAVA_HOME = /opt/homebrew/opt/openjdk`.
1. The `smili` workflow fails with an `Illegal Instruction` error. To fix this disable `Use Rosetta for x86_64/amd64 emulation on Apple Silicon` checkbox in Docker settings.
1. The `difmap` workflow might fail if the `Use Rosetta for x86_64/amd64 emulation on Apple Silicon` checkbox is disabled Docker settings.
