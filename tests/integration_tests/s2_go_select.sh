#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    echo "gadi - NCI"
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles

    module load ard-scene-select-py3-dea/20221025
    
    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    SSPATH="$SCRIPT_DIR/../.."
else
    echo "not NCI"
    SSPATH=$HOME/sandbox/dea-ard-scene-select
fi

TEST_DATA=/g/data/u46/users/dsg456/test_data
yamdir=' --yamls-dir '$TEST_DATA'/s2/autogen/yaml'

# so it uses the dev scene select
#echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH

scratch=scratch_s2
mkdir -p $scratch

pkgdir=$scratch/pkgdir$RANDOM
mkdir -p $pkgdir

# local ard_scene_select.py
# test db read
# ard processing
# test DB index

python3 $SSPATH/scene_select/ard_scene_select.py  --config ${USER}_dev.conf --products '["esa_s2am_level1_0"]' $yamdir  --workdir $scratch/  --pkgdir  $pkgdir --logdir $scratch/ --env $PWD/s2_interim_prod_wagl.env --project u46 --walltime 02:30:00  --index-datacube-env index-test-odc.env --interim-days-wait 5 --run-ard

