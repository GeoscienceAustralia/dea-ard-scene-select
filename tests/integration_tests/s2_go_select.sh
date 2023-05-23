#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    echo "gadi - NCI"
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles
    module use /g/data/u46/users/$USER/devmodules/modulefiles

    # Needed for pytest to be loaded
    # module load dea/20221025
    # module load ard-scene-select-py3-dea/dev_20230522
    module load ard-scene-select-py3-dea/20230330
    
    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    SSPATH="$SCRIPT_DIR/../.."
else
    echo "not NCI"
    SSPATH=$HOME/sandbox/dea-ard-scene-select
fi

script_directory=$(dirname $(dirname "$(readlink -f "$0")"))
TEST_DATA="$script_directory/test_data/integration_tests/"
yamdir=" --yamls-dir '$TEST_DATA'/s2/autogen/yaml "

# so it uses the dev scene select
# echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

scratch=scratch_s2
mkdir -p $scratch

pkgdir=$scratch/pkgdir$RANDOM
mkdir -p $pkgdir

# local ard_scene_select.py
# test db read
# ard processing
# test DB index

python3 $SSPATH/scene_select/ard_scene_select.py  --config ${USER}_dev.conf --products '["esa_s2am_level1_0"]' $yamdir  --workdir $scratch/  --pkgdir  $pkgdir --logdir $scratch/ --env $PWD/s2_interim_prod_wagl.env --project u46 --walltime 02:30:00  --interim-days-wait 5  --index-datacube-env index-test-odc.env # --run-ard

