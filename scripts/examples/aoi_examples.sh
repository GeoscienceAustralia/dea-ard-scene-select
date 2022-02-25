#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
	echo If module load breaks check on a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/dsg547/devmodules/modulefiles


	#module load ard-scene-select-py3-dea/20211115
	module load ard-scene-select-py3-dea/20220121

	#module load h5-compression-filters/20200612
fi

mkdir -p scratch

SSPATH=$PWD/../../

#PRODUCTS = '["ga_ls5t_level1_3", "ga_ls7e_level1_3", \
#     "usgs_ls5t_level1_1", "usgs_ls7e_level1_1", "usgs_ls8c_level1_1"]'

# You'll need a scratch directory
# mkdir scratch

# so it uses the dev scene select
# echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

# The module
#time ard-scene-select --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env  --products '["usgs_ls7e_level1_1"]' --scene-limit 999999 #--find-blocked

mkdir -p scratch
ard_loc='/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env'

time python3 ../../scene_select/ard_scene_select.py --workdir scratch/ --pkgdir scratch/ --logdir scratch/ --project u46 --walltime 00:10:00  --env $ard_loc --allowed-codes ../../scene_select/data/Australian_wrs_list_optimal_v3.txt --products '["usgs_ls5t_level1_1"]'

#  grep AOI ard_scene_select.log | wc -l
# 2759

# Using the new code...
# grep AOI /g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/examples/scratch/filter-jobid-7c0bf6/ard_scene_select.log | wc -l
# 2759

# python3 ../../scene_select/ard_scene_select.py --help
