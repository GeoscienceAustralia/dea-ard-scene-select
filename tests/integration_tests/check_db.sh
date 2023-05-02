#!/usr/bin/env bash



if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea
  #module load ard-pipeline/devv2.1
  ODCCONF="--config ${USER}_dev.conf"
  
else
  echo "not NCI"
  ODCCONF="--config ${USER}_local.conf"
  # datacube -v  $ODCCONF system init
fi

datacube  $ODCCONF product list # 


echo product='esa_s2am_level1_0'
datacube  $ODCCONF dataset search product='esa_s2am_level1_0' | grep '^id: '

echo product='esa_s2bm_level1_0'
datacube  $ODCCONF dataset search product='esa_s2bm_level1_0' | grep '^id: '

echo product='ga_s2am_ard_3'
datacube  $ODCCONF dataset search product='ga_s2am_ard_3' | grep '^id: '

echo product='ga_s2bm_ard_3'
datacube  $ODCCONF dataset search product='ga_s2bm_ard_3' | grep '^id: '

echo product='usgs_ls7e_level1_1'
datacube  $ODCCONF dataset search product='usgs_ls7e_level1_1' | grep '^id: '

echo product='usgs_ls8c_level1_1'
datacube  $ODCCONF dataset search product='usgs_ls8c_level1_1' | grep '^id: '

echo product='usgs_ls8c_level1_2'
datacube  $ODCCONF dataset search product='usgs_ls8c_level1_2' | grep '^id: '

echo product='ga_ls8c_ard_3'
datacube  $ODCCONF dataset search product='ga_ls8c_ard_3' | grep '^id: '

echo product='ga_ls7e_ard_3'
datacube  $ODCCONF dataset search product='ga_ls7e_ard_3' | grep '^id: '