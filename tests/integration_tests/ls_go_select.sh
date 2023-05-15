#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    echo "gadi - NCI"
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles

    #module load ard-scene-select-py3-dea/20211115
    module load ard-scene-select-py3-dea/20230515

    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    SSPATH="$SCRIPT_DIR/../.."
else
    echo "not NCI"
    SSPATH=$HOME/sandbox/dea-ard-scene-select
fi


# so it uses the dev scene select
# echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

scratch=scratch_ls
mkdir -p $scratch

pkgdir=$scratch/pkgdir$RANDOM
mkdir -p $pkgdir

# local ard_scene_select.py
# test db read
# ard processing
# test DB index
# -products '["usgs_ls8c_level1_2"]'

# '["usgs_ls8c_level1_2", "esa_s2am_level1_0"]'

python3 $SSPATH/scene_select/ard_scene_select.py  --config ${USER}_dev.conf --products '["usgs_ls7e_level1_1", "usgs_ls8c_level1_1", "usgs_ls8c_level1_2", "usgs_ls9c_level1_2"]' --workdir $scratch/  --pkgdir  $pkgdir --logdir $scratch/ --env $PWD/ls_interim_prod_wagl.env --project u46 --walltime 02:30:00 --interim-days-wait 5 --allowed-codes Australian_AOI_107069_added.json --days-to-exclude '["2009-01-03:2009-01-05"]' --index-datacube-env index-test-odc.env # --run-ard

#ard-scene-select  --config dsg547_dev.conf --products '["usgs_ls8c_level1_2", "usgs_ls7e_level1_2"]' --workdir $scratch/  --pkgdir  $scratch/ --logdir $scratch/ --env $PWD/interim-prod-wagl.env --project u46 --walltime 02:30:00  --index-datacube-env index-test-odc.env  --interim-days-wait 5 --run-ard
