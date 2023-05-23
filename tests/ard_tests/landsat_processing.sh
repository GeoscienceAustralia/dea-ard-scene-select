#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=00:30:00,mem=155GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20230330

PRODUCTS='["usgs_ls9c_level1_2"]'
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SCRATCH=$DIR"/scratch"
mkdir -p $SCRATCH

pkgdir=$SCRATCH/pkgdir$RANDOM
mkdir -p $pkgdir

PRODWAGLLS="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"
ENV_FILE="$DIR/index-test-odc.env"
ard-scene-select  --config ${USER}_dev.conf \
   --workdir $SCRATCH \
   --pkgdir  $pkgdir \
   --logdir $SCRATCH  \
   --env $PRODWAGLLS  \
   --index-datacube-env $ENV_FILE \
   --products $PRODUCTS \
   --project u46 \
   --walltime 05:00:00 \
   --run-ard #--index-datacube-env c3-samples-index-datacube.env
