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
#module use /home/547/dsg547/devmodules/modulefiles

#module load ard-scene-select-py3-dea/20201126

module load ard-scene-select-py3-dea/20200909
module load h5-compression-filters/20200612
module load dea

[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"

# For testing as a non-qsub execution
INIT_PWD=$PWD

# This should work if scratch/ga_ls8c_ard_3/ is empty
#python3 ../../../scene_select/ard_scene_select.py --usgs-level1-files /g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/tests/scripts/non-prod/old_ard.txt --products '["usgs_ls8c_level1_1"]' --workdir $INIT_PWD/scratch/ --pkgdir $INIT_PWD/scratch/ --logdir $INIT_PWD/scratch/ --env $INIT_PWD/prod-wagl.env --index-datacube-env $INIT_PWD/dsg547_dev.env  --project u46 --walltime 01:00:00  --config $INIT_PWD/dsg547_dev.conf

# This crashes
python3 ../../../scene_select/ard_scene_select.py --usgs-level1-files /g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/tests/scripts/non-prod/old_ard.txt --products '["usgs_ls8c_level1_1"]' --workdir $INIT_PWD/scratch/ --pkgdir /g/data/u46/users/dsg547/test_data/c3/ --logdir $INIT_PWD/scratch/ --env $INIT_PWD/prod-wagl.env --index-datacube-env $INIT_PWD/dsg547_dev.env  --project u46 --walltime 01:00:00  --config $INIT_PWD/dsg547_dev.conf

# local dev
#python3 ../../../scene_select/ard_scene_select.py --products '["usgs_ls8c_level1_1"]' --workdir $INIT_PWD/scratch/ --pkgdir $INIT_PWD/scratch/ --logdir $INIT_PWD/scratch/ --env $INIT_PWD/prod-wagl.env --index-datacube-env $INIT_PWD/dsg547_dev.env  --project u46 --walltime 01:00:00  --config $INIT_PWD/dsg547_dev.conf --find-blocked

# A module
#ard-scene-select --products '["usgs_ls8c_level1_1"]' --workdir $INIT_PWD/scratch/ --pkgdir $INIT_PWD/scratch/ --logdir $INIT_PWD/scratch/ --env $INIT_PWD/prod-wagl.env --index-datacube-env $INIT_PWD/dsg547_dev.env  --project u46 --walltime 01:00:00  --config $INIT_PWD/dsg547_dev.conf  # --run-ard # note will try and index

# to use the dev db
#  --config $INIT_PWD/dsg547_dev.conf --products '["usgs_ls8c_level1_1"]'