#!/bin/bash

# ssh lpgs@gadi-dm.nci.org.au /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/submit_ard_prod.sh

# location of the script and environment files used in production
CODE_DIR='/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/tests/non-prod'

# Capturing the .e & .o files in a run dir
RUN_DIR=$CODE_DIR/scratch/logdir

cd $RUN_DIR

PBS_LOG=$RUN_DIR/submit_ard_prod.log

qsub -v INIT_PWD=$CODE_DIR $CODE_DIR/job_ard_prod.sh >>$PBS_LOG 2>&1
