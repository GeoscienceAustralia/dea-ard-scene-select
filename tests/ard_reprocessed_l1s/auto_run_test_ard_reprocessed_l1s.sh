#!/usr/bin/env bash

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"

  module use /g/data/v10/public/modules/modulefiles;
  
  module use /g/data/v10/private/modules/modulefiles;
  # Needed for pytest to be loaded
  module load dea/20221025
  # module load ard-scene-select-py3-dea/dev_20230606
  # module load ard-scene-select-py3-dea/20230615

  echo "loaded the necessary packages as we run on nci gadi"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../..

# so it uses the repo scene select
# Comment these lines out to use the module
# echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

python3 -m pytest -v -s auto_test_ard_reprocessed_l1s.py  # -k test_ard_reprocessed_l1s
