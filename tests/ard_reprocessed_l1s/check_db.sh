#!/usr/bin/env bash



if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea
  #module load ard-pipeline/devv2.1

  TEST_DATA=/g/data/u46/users/${USER}/test_data
  ODCCONF="--config ${USER}_dev.conf"
  
else
  echo "not NCI"
  ODCCONF="--config ${USER}_local.conf"
  TEST_DATA=$HOME/test_data
  # datacube -v  $ODCCONF system init
fi

datacube  $ODCCONF product list

echo product='usgs_ls9c_level1_2'
datacube  $ODCCONF dataset search product='usgs_ls9c_level1_2' | grep '^id: '

echo product='ga_ls9c_ard_3'
datacube  $ODCCONF dataset search product='ga_ls9c_ard_3' | grep '^id: '
