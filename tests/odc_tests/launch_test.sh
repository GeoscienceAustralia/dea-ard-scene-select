#!/usr/bin/env bash

# This is a test launcher whereby we can give it any test 
# or *.py to run on pytest

host=localhost
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles
  fi

  #module load dea/20231123

  # This is useful when testing a new ard-scene-select module
  # Comment out the export PYTHONPATH line below
  module load ard-scene-select-py3-dea/dev_20231130
  host=deadev.nci.org.au

fi

export ODC_TEST_DB_URL=postgresql://$USER"@"$host"/"$USER"_automated_testing"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../..

[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH
#export PYTHONPATH=$PYTHONPATH

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
