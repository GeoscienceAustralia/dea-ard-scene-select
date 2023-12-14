#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/$USER/devmodules/modulefiles

	# module load ard-scene-select-py3-dea/20220516
	# module load ard-scene-select-py3-dea/20231010
	# module load ard-scene-select-py3-dea/20231205
	module load dea/20221025
	#module load dea/20231204
fi

mkdir -p scratch

SSPATH=$PWD/../../
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
export PYTHONPATH=$PYTHONPATH


PRODUCTS='["esa_s2am_level1_0","esa_s2bm_level1_0"]'
PRODUCTS='["esa_s2bm_level1_0"]'
ARD_ENV="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-s2.env"

#PRODUCTS='["usgs_ls8c_level1_2","usgs_ls9c_level1_2"]'
#ARD_ENV="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"

# The module scene-select
#time ard-scene-select --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00 --env $ARD_ENV  --products $PRODUCTS --scene-limit 999999 #--find-blocked

# Scene select in this repo
time python3 ../../scene_select/ard_scene_select.py --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env $ARD_ENV  --products $PRODUCTS #--use-viirs-after 2023-07-03 #--find-blocked

# using a wrt list
#python3 ../../scene_select/ard_scene_select.py  --usgs-level1-files ../../tests/test_data/All_Landsat_Level1_Nci_Files.txt  --workdir scratch/ --allowed-codes ../../tests/test_data/standard_wrs_list.txt  --logdir scratch/
