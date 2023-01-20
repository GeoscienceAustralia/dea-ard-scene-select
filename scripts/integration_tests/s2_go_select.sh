#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    echo "gadi - NCI"
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles

    module load ard-scene-select-py3-dea/20221025
    
    TEST_DATA=/g/data/u46/users/dsg547/test_data
    SSPATH=/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select
    yamdir=' --yamls-dir '$TEST_DATA'/s2/autogen/yaml'
else
    echo "not NCI"
    SSPATH=$HOME/sandbox/dea-ard-scene-select
fi


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
# -products '["usgs_ls8c_level1_2"]'

# '["usgs_ls8c_level1_0", "esa_s2am_level1_0"]'



# This will fail until the yaml root location is passed in. - But it is passed in: $yamdir
# Turning off the ard processing for now - This is just a scene select test
python3 $SSPATH/scene_select/ard_scene_select.py  --config dsg547_dev.conf --products '["esa_s2am_level1_0"]' --yamls-dir /g/data/u46/users/dsg547/test_data/c3/s2_autogen/yaml  --workdir $scratch/  --pkgdir  $pkgdir --logdir $scratch/ --env $PWD/s2_interim_prod_wagl.env --project u46 --walltime 02:30:00  --index-datacube-env index-test-odc.env $yamdir  --interim-days-wait 5 #--run-ard
