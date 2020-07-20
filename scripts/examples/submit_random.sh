#!/bin/bash

# ssh lpgs@gadi-dm.nci.org.au /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/examples/submit_random.sh
# Capturing the .e & .o files in a run dir
INIT_PWD=$PWD
RUN_DIR='/g/data/v10/projects/c3_ard/test'
#RUN_DIR='/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/scripts/examples/scratch'
cd $RUN_DIR
PBS_LOG=$RUN_DIR/submit_ard_prod.log
qsub -v INIT_PWD=$INIT_PWD $INIT_PWD/random.sh >>$PBS_LOG 2>&1
