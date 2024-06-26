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

module load ard-scene-select-py3-dea/20220922

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SCRATCH=${DIR}/scratch
mkdir -p $SCRATCH

# The collection 1 products
# PRODUCTS='["usgs_ls5t_level1_1","usgs_ls7e_level1_1","usgs_ls8c_level1_1"]'

# PRODUCTS='["usgs_ls8c_level1_2","usgs_ls9c_level1_2"]'

ard-scene-select \
--workdir  $SCRATCH \
--pkgdir  $SCRATCH \
--logdir $SCRATCH \
--project u46 \
--products $PRODUCTS \
--brdfdir  /g/data/v10/eoancillarydata-2/BRDF/MCD43A1.061 \
--allowed-codes /g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/examples/Australian_AOI_ls_extended.json \
--find-blocked \
