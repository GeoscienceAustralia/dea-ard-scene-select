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
  module load dea/20221025

  # module load ard-scene-select-py3-dea/dev_20230522
  module load ard-scene-select-py3-dea/20230616  # This is from ls_go_select.sh
  host=deadev.nci.org.au

fi

#export the following so that the s2 tests can directly manipulate the
# local datacube with regards to dataset
export ODC_DB=$USER"_automated_testing"
export ODC_TEST_DB_URL=postgresql://$USER"@"$host":"$port"/"$ODC_DB
export ODC_HOST=$host

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
  echo "duncans laptop"
  echo "conda activate /home/duncan/bin/miniconda3/envs/odc2020"
  echo "note the conda env is broken"
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
