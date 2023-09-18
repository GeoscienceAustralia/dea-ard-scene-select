#!/usr/bin/env bash

# This is a test launcher whereby we can give it any test 
# or *.py to run on pytest

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
 
  module use /g/data/v10/public/modules/modulefiles;
 
  module use /g/data/v10/private/modules/modulefiles;
  # Needed for pytest to be loaded
  module load dea/20221025
  # module load ard-scene-select-py3-dea/dev_20230606
  # module load ard-scene-select-py3-dea/20230615
 
  echo "loaded the necessary packages as we run on nci gadi"
 
  export ODC_TEST_DB_URL=postgresql://$USER"@deadev.nci.org.au/"$USER"_automated_testing"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../../

[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH
export PYTHONPATH=$PYTHONPATH

if [ -e "$1" ] && [ -n "$1" ]; then
  # Run the specific file if it exists in the args
  # and not an empty string
  echo " Going to run test, '$1'..."
  python3 -m pytest -v -s $1
  echo " Completed running test, '$1'..."
else
  echo "Running all python test files"
  python3 -m pytest -v -s test*.py
fi
