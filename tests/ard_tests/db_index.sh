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

# Defining ard metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel_ard.odc-type.yaml

# Defining S2 l1's
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel.odc-type.yaml
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/esa_s2am_level1_0.odc-product.yaml

# Defining S2 ard's
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/ga_s2am_ard_3.odc-product.yaml

# Defining ls9 l1 c1
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls9.odc-product.yaml

# Defining ls9 ard
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls9.odc-product.yaml

# Defining ls8 l1
# c1
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8.odc-product.yaml

# c2
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8_c2.odc-product.yaml

# Defining ls8 ard
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls8.odc-product.yaml

# ls9 - The tar is from /g/data/da82/AODH/USGS/L1/Landsat/C2/097_075/LC90970752022239
# moved to /g/data/u46/users/dsg547/test_data/c3/LC90970752022239/
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.odc-metadata.yaml

# S2 - don't know where the tar is
# id:
# datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-01/15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.odc-metadata.yaml

# id: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.odc-metadata.yaml
