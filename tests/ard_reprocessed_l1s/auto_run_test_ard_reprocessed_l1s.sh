#!/usr/bin/env bash

host=localhost
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"

  module use /g/data/v10/public/modules/modulefiles;
  module use /g/data/v10/private/modules/modulefiles;
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles
  fi
  module load ard-scene-select-py3-dea/dev_20231130
  #module load dea/20231123

  echo "Loaded the necessary packages as we run on nci gadi"
  host=deadev.nci.org.au
fi

export ODC_TEST_DB_URL=postgresql://$USER"@"$host"/"$USER"_automated_testing"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../..

# The line below ensures we use the copy of scene_select.ard_reprocessed_l1s
# in this repository.
# Comment these lines out to use the module installed system wide. 
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#export PYTHONPATH=$PYTHONPATH

python3 -m pytest -v -s auto_test_ard_reprocessed_l1s.py
