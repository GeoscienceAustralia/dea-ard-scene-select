#!/usr/bin/env bash

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"

  module use /g/data/v10/public/modules/modulefiles;
  module use /g/data/v10/private/modules/modulefiles;
  module load dea/20221025

  echo "Loaded the necessary packages as we run on nci gadi"

  export ODC_TEST_DB_URL=postgresql://$USER"@deadev.nci.org.au/"$USER"_automated_testing"
else
  export ODC_TEST_DB_URL=postgresql://$USER"@localhost/"$USER"_automated_testing"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../..

# The line below ensures we use the copy of scene_select.ard_reprocessed_l1s
# in this repository.
# Comment these lines out to use the module installed system wide. 
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"

python3 -m pytest -v -s auto_test_ard_reprocessed_l1s.py
