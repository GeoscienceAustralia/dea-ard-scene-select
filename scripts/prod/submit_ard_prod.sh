#!/bin/bash

# Capturing the .e & .o files in a run dir
INIT_PWD=$PWD
RUN_DIR='/g/data/v10/work/c3_ard/logdir/jobdir'
cd $RUN_DIR
qsub $INIT_PWD/job_ard_prod.sh
