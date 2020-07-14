#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=0:30:00,mem=15GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

# Note it is assumed the user lpgs will be executing this script with;
# qsub ard_production

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20200709

# local work dir and skipping odc
../scene_select/ard_scene_select.py --workdir $PWD/workdir --pkgdir $PWD/pkgdir --logdir $PWD/logdir --env $PWD/prod-wagl.env --index-datacube-env $PWD/c3-samples-index-datacube.env  --project u46 --walltime 03:00:00 #--run-ard

# This flag is handy for doing a test and you want a result quickly
#  --products '["ga_ls5t_level1_3"]'
