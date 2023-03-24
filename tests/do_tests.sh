#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

# Does not seem to be needed, but this has be flakey in the past
# so I'm leaving it in for now as a comment
# module load h5-compression-filters/20200612

module load ard-pipeline/20230306-l9

SSPATH=$PWD/../

# so it uses the dev scene select
#echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH

pytest -s test_ard_scene_select.py
pytest -s test_check_ancillary.py #-k 'test_ancillaryfiles_actual'
pytest -s test_do_ard.py
pytest -s test_utils.py
#./timing_check_ancillary.py
