#!/bin/bash

DIR=dags
UVFITSDIR=$1
SCRIPTSDIR=$2

rm -f workflow.yml eht.png
./workflow.py -u $UVFITSDIR -s $SCRIPTSDIR | grep -v defaultdict > workflow.yml

pegasus-graphviz --label=xform \
                 --files \
                 --output eht.png \
                 workflow.yml &> /dev/null

# Plan and submit the workflow
pegasus-plan --conf pegasus.properties \
             --sites condorpool \
             --output-dir ${DIR}/output \
             --dir ${DIR} \
             --cleanup leaf \
             --force \
             --submit \
             workflow.yml

# Replace docker_init with container_init
# find ${DIR} -name '*.sh' -exec sed -E -i '' -e "s@^docker_init (.*)@container_init ; cont_image='pegasus/reproducibility-eht:\1'@g" {} \;
