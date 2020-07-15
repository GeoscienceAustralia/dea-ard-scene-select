#!/bin/bash
#PBS -P v10
#PBS -W umask=017
#PBS -q normal
#PBS -l walltime=0:30:00,mem=15GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

# Note it is assumed the user lpgs will be executing this script with;
# qsub ard_production.sh

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20200715

# local work dir and skipping odc
ard-scene-select --workdir /g/data/v10/work/c3_ard/workdir --pkgdir /g/data/xu18/ga --logdir /g/data/v10/work/c3_ard/logdir --env $PWD/prod-wagl.env --index-datacube-env $PWD/index-datacube.env  --project v10 --walltime 05:00:00 #--run-ard

