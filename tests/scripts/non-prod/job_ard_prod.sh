#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q normal
#PBS -l walltime=0:30:00,mem=15GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

# Note it is assumed the user lpgs will be executing this script via;
# ./submit_ard_prod.sh
# Note the submit_ard_prod.sh sets the $INIT_PWD variable 

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20200821

# For testing as a non-qsub execution
INIT_PWD=$PWD

ard-scene-select --workdir $INIT_PWD/scratch/ --pkgdir $INIT_PWD/scratch/ --logdir $INIT_PWD/scratch/ --env $INIT_PWD/prod-wagl.env --index-datacube-env $INIT_PWD/index-datacube.env  --project u46 --walltime 01:00:00 # --run-ard note will try and index

# to use the dev db
#  --config $INIT_PWD/dsg547_dev.conf --products '["usgs_ls8c_level1_1"]'
