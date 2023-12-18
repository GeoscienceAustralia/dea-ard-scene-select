#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles
  fi

    # Does not seem to be needed, but this has be flakey in the past
    # so I'm leaving it in for now as a comment
    # module load h5-compression-filters/20200612

    #module load ard-pipeline/20230306-l9
    #module load ard-scene-select-py3-dea/dev_20231130

    # tried this but it did not work.
    # test_check_ancillary.py fails
    # module load dea/20221025
    # module load h5-compression-filters/20230215
    # module load dea/20231123

    # This is useful when testing a new ard-scene-select module
    # Comment out the export PYTHONPATH line below
    # module load ard-scene-select-py3-dea/dev_20231130
    module load ard-scene-select-py3-dea/20231205

fi


if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
    echo "duncans laptop"
    echo "conda activate /home/duncan/bin/miniconda3/envs/odc2020"
    echo "test_check_ancillary.py fails with No module named 'wagl'"
fi

SSPATH=$PWD/../

# so it uses the dev scene select
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#export PYTHONPATH=$PYTHONPATH

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pytest -s -v $SCRIPT_DIR/test_*.py

#./timing_check_ancillary.py
