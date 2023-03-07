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
#module use /home/547/dsg547/devmodules/modulefiles

#module load ard-scene-select-py3-dea/20200717  # fast no ancillary file checking
#module load ard-scene-select-py3-dea/20200811  # crap and slow
#module load ard-scene-select-py3-dea/20200813  # Fast.
module load ard-scene-select-py3-dea/20220922

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ard-scene-select \
--workdir scratch/  \
--pkgdir  scratch/ \
--logdir scratch/ \
--env $DIR/brdf61_prod-wagl.env \
--project u46 \
--brdfdir  /g/data/v10/eoancillarydata-2/BRDF/MCD43A1.061 \
#--find-blocked \
# --run-ard \
--walltime 05:00:00
