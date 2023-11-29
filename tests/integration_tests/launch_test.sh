#!/usr/bin/env bash

# This is a test launcher whereby we can give it any test
# or *.py to run on pytest

host=localhost
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles   # This is from ls_go_select.sh
  fi

  # the  3 modules/packages ard-scene-select is dependant on;
  # dea module
  # h5-compression-filters module
  # pytest-odc package
  #module load dea/20221025
  module load dea/20231123
  #module load h5-compression-filters/20230215

  # This is useful when testing a new ard-scene-select module
  # Comment out the export PYTHONPATH line below
  # module load ard-scene-select-py3-dea/20231010
  host=deadev.nci.org.au

fi

#export the following so that the s2 tests can directly manipulate the
# local datacube with regards to dataset
export ODC_DB=$USER"_automated_testing"
export ODC_TEST_DB_URL=postgresql://$USER"@"$host"/"$ODC_DB
export ODC_HOST=$host
export DATACUBE_DB_URL=$ODC_TEST_DB_URL

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
  echo "duncans laptop"
  echo "conda activate dea2023"
  echo "sudo service postgresql start"
fi


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../../

[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH
export PYTHONPATH=$PYTHONPATH


cd "$(dirname "$0")"

if [ -e $1 ]; then
  echo " Going to run $1"
  python3 -m pytest -v -s $1
else
  echo "ERROR - $1 does not exist"
fi
