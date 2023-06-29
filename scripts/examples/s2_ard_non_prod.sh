#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q normal
#PBS -l walltime=2:30:00,mem=15GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20230330

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SCRATCH=${DIR}/scratch
mkdir -p $SCRATCH

PKDIR=$SCRATCH/pkgdir$RANDOM
mkdir -p $PKDIR

ard-scene-select \
--workdir  $SCRATCH \
--pkgdir  $PKDIR \
--logdir $SCRATCH \
--project u46 \
--usgs-level1-files small_S2.txt \
--env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-s2.env \
--yamls-dir /g/data/ka08/ga/l1c_metadata \
--run-ard


