#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEST_DATA_REL="${SCRIPT_DIR}/../test_data/ls9_reprocessing"
TEST_DATA=$(realpath "$TEST_DATA_REL")

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20221025
else
  echo "Not on NCI"
fi

SSPATH=$DIR/../..

# so it uses the dev scene select
echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
echo $PYTHONPATH


# delete and recreate the file structure
rm -rf $TEST_DATA/moved/ga_ls9c_ard_3/
rm -rf $TEST_DATA/ga_ls9c_ard_3/
cp -r $TEST_DATA/a_ga_ls9c_ard_3_raw/ $TEST_DATA/ga_ls9c_ard_3/
mkdir -p $TEST_DATA/moved/
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p $DIR/scratch/   # for test logs
