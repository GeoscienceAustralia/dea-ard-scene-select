#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/$USER/devmodules/modulefiles

	module load ard-scene-select-py3-dea/20220516
fi

mkdir -p scratch;

SSPATH=$PWD/../../
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"

# PRODUCTS='["esa_s2am_level1_0","esa_s2bm_level1_0"]'
# ARD_ENV="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-s2.env"

ARD_ENV="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"

# The module scene-select
# time ard-scene-select --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  \
# --env $ARD_ENV  --products $PRODUCTS --scene-limit 999999 #--find-blocked

# Scene select in this repo
time python3 ../../scene_select/ard_reprocessed_l1s.py --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46  \
--env $ARD_ENV  --scene-limit 1 --dry-run
