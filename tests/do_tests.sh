#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles

    # Does not seem to be needed, but this has be flakey in the past
    # so I'm leaving it in for now as a comment
    # module load h5-compression-filters/20200612

    module load ard-pipeline/20230306-l9
fi


if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
    echo "duncans laptop"
    echo "conda activate /home/duncan/bin/miniconda3/envs/odc2020"
    echo "test_check_ancillary.py fails with No module named 'wagl'"
fi

SSPATH=$PWD/../

# so it uses the dev scene select
#echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pytest -s -v $SCRIPT_DIR/test_*.py

#./timing_check_ancillary.py

# Clean up for test_ard_scene_select.py
rm -rf temp_jobdir_testing_* temp_logdir_testing_*
