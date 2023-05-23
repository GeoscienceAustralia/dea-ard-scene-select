#!/usr/bin/env bash

# start a local postgres
# sudo service postgresql start


if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20221025
  #module load ard-pipeline/devv2.1
  ODCCONF="--config ${USER}_dev.conf"
  echo "Using local user's config, ${ODCCONF}"

  TEST_DATA="/g/data/u46/users/dsg547/test_data"
  
else
  echo "not NCI"
  ODCCONF="--config ${USER}_local.conf"
  # datacube -v  $ODCCONF system init
fi

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
    echo "This needs to be run on the NCI"
fi

# Defining landsat l1 metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_l1.odc-type.yaml

# Defining ls9 l1 c1
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls9.odc-product.yaml

# Defining ard metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml

# Defining ls9 ard
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls9.odc-product.yaml

# ls9 - The tar is from /g/data/da82/AODH/USGS/L1/Landsat/C2/097_075/LC90970752022239
# moved to /g/data/u46/users/dsg547/test_data/c3/LC90970752022239/
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.odc-metadata.yaml


