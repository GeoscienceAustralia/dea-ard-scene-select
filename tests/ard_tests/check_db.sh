#!/usr/bin/env bash
source ../dynamic_config_file.sh

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea
  #module load ard-pipeline/devv2.1
  generate_dynamic_config_file "gadi"
else
  generate_dynamic_config_file
fi

ODCCONF="--config ${USER}_dev.conf"

echo product='esa_s2am_level1_0'
datacube  $ODCCONF dataset search product='esa_s2am_level1_0' | grep '^id: '

echo product='ga_s2am_ard_3'
datacube  $ODCCONF dataset search product='ga_s2am_ard_3' | grep '^id: '

echo product='usgs_ls9c_level1_2'
datacube  $ODCCONF dataset search product='usgs_ls9c_level1_2' | grep '^id: '

echo product='ga_ls9c_ard_3'
datacube  $ODCCONF dataset search product='ga_ls9c_ard_3' | grep '^id: '

clean_up_dynamic_config_file
