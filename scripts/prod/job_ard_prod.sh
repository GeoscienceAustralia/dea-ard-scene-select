#!/bin/bash
#PBS -P v10
#PBS -W umask=017
#PBS -q copyq
#PBS -l walltime=3:00:00,mem=15GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

# Note it is assumed the user lpgs will be executing this script via;
# ./submit_ard_prod.sh
# Note the submit_ard_prod.sh sets the $INIT_PWD variable

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

#module load ard-scene-select-py3-dea/20210216
module load ard-scene-select-py3-dea/20231010

ard-scene-select --workdir /g/data/v10/work/c3_ard/workdir --pkgdir /g/data/xu18/ga --logdir /g/data/v10/work/c3_ard/logdir --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env --index-datacube-env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/index-datacube.env  --project v10 --walltime 10:00:00 #--run-ard --products '["usgs_ls7e_level1_1"]' --find-blocked
