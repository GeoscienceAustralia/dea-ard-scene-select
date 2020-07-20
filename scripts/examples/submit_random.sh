#!/bin/bash

# This will work on gadi
# ssh lpgs@gadi-dm.nci.org.au /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/examples/submit_random.sh

# This will not work on gadi
# But should work everywehere else
#ssh lpgs@gadi.nci.org.au /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/examples/submit_random.sh

# Capturing the .e & .o files in a run dir
CODE_DIR='/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/scripts/prod'
CODE_DIR='/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/examples'
RUN_DIR='/g/data/v10/projects/c3_ard/test'
#RUN_DIR='/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/scripts/examples/scratch'
cd $RUN_DIR
PBS_LOG=$RUN_DIR/submit_ard_prod.log
qsub -v INIT_PWD=$CODE_DIR $CODE_DIR/random.sh >>$PBS_LOG 2>&1
