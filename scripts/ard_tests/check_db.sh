#!/usr/bin/env bash

db_hostname="localhost"
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20231123

  db_hostname="deadev.nci.org.au"
fi
export DATACUBE_DB_URL=postgresql://$USER"@"$db_hostname"/"$USER"_dev"


echo product='esa_s2am_level1_0'
datacube   dataset search product='esa_s2am_level1_0' | grep '^id: '

echo product='ga_s2am_ard_3'
datacube   dataset search product='ga_s2am_ard_3' | grep '^id: '

echo product='usgs_ls9c_level1_2'
datacube   dataset search product='usgs_ls9c_level1_2' | grep '^id: '

echo product='ga_ls9c_ard_3'
datacube   dataset search product='ga_ls9c_ard_3' | grep '^id: '

