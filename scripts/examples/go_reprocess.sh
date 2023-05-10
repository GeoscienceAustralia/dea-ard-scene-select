#!/bin/bash

if [[ "$HOSTNAME" == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/$USER/devmodules/modulefiles

	module load ard-scene-select-py3-dea/dev_20230510

fi

# Run the local scene select
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SSPATH="$DIR"/../..
#SSPATH=$script_directory
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
echo $PYTHONPATH

SCRATCH="$DIR"/scratch/
mkdir -p "$SCRATCH"

# PRODUCTS='["esa_s2am_level1_0","esa_s2bm_level1_0"]'
# ARD_ENV="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-s2.env"

ARD_ENV="$DIR"/../prod/ard_env/prod-wagl-ls.env

# ard_reprocessed_l1s.py in this repo
#time python3 "$DIR"/../../scene_select/ard_reprocessed_l1s.py --workdir "$SCRATCH"  --pkgdir  "$SCRATCH" --logdir "$SCRATCH" --project u46  --env $ARD_ENV  --scene-limit 1 --dry-run

# ard-reprocessed-l1s module
ard-reprocessed-l1s --workdir "$SCRATCH"  --pkgdir  "$SCRATCH" --logdir "$SCRATCH" --project u46  --env $ARD_ENV  --scene-limit 1 --dry-run
