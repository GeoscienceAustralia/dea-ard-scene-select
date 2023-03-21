#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
	echo "If module load breaks check out a clean environment"
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/gy5636/devmodules/modulefiles # This refers to the dev package we created for testing
	module use /g/data/u46/users/gy5636/modules/modulefiles # this should load up the h5 as the h5 is in "modules/modulefiles/h5-compression-filters/20230215"
	module load ard-scene-select-py3-dea/dev_20230322 # Gordon - Dev - this is the package created on 20230223
	echo "Just loaded the new module we are testing - 'load ard-scene-select-py3-dea/dev_20230322'"
	echo "Performing ODCCONF system check now..."
	echo $ODCCONF
	datacube $ODCCONF system check
	echo "ODCCONF - System check done"
fi

# observe the scratch directory below that is named against the 
# current ticket we are working on
mkdir -p scratch_DSNS_102

SSPATH=$PWD/../../
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"

# The module
time ard-scene-select --workdir scratch_DSNS_102/  --pkgdir  scratch_DSNS_102/ --logdir scratch_DSNS_102/ --project u46 --walltime 10:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env --products '["usgs_ls9c_level1_2"]' --scene-limit 999999 #--find-blocked

echo "Done running ard scene select to test...."