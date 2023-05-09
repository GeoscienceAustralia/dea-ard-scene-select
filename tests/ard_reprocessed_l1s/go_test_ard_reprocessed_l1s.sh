#!/usr/bin/env bash

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20221025
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SSPATH=$DIR/../..

# so it uses the repo scene select
echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
echo $PYTHONPATH

# Getting the scene select call to 
# use the right database
# export DATACUBE_CONFIG_PATH=$DIR/datacube.conf
# export DATACUBE_ENVIRONMENT=$ODCDB

python3 -m pytest test_ard_reprocessed_l1s.py