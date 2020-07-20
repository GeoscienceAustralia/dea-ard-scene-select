#!/bin/bash

# ssh lpgs@gadi-dm.nci.org.au /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/examples/submit_random.sh
# Capturing the .e & .o files in a run dir
INIT_PWD=$PWD
RUN_DIR='/g/data/v10/projects/c3_ard/test'
cd $RUN_DIR
qsub -v INIT_PWD=$INIT_PWD $INIT_PWD/random.sh >  $RUN_DIR/submit_ard_prod.log 2>$RUN_DIR/submit_ard_prod..err
