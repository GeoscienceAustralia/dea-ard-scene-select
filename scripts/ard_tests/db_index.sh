#!/usr/bin/env bash

# start a local postgres
# sudo service postgresql start

db_hostname="localhost"
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles
  fi

  #module load dea/20221025
  module load dea/20231123
  #module load ard-pipeline/devv2.1
  db_hostname="deadev.nci.org.au"
else
  echo "not NCI"
fi
export DATACUBE_DB_URL=postgresql://$USER"@"$db_hostname"/"$USER"_dev"

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
  echo "duncans laptop"
  echo "conda activate dea2023"
  echo "sudo service postgresql start"
fi


datacube system check
# Defining landsat l1 metadata
datacube  metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_l1.odc-type.yaml

# Defining ard metadata
datacube  metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube  metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel_ard.odc-type.yaml

# Defining S2 l1's
datacube  metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel.odc-type.yaml
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/esa_s2am_level1_0.odc-product.yaml
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/esa_s2bm_level1_0.odc-product.yaml

# Defining S2 ard's
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/ga_s2am_ard_3.odc-product.yaml
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/ga_s2bm_ard_3.odc-product.yaml

# Defining ls9 l1 c1
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls9.odc-product.yaml

# Defining ls9 ard
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls9.odc-product.yaml

# Defining ls8 l1
# c1
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8.odc-product.yaml

# c2
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8_c2.odc-product.yaml

# Defining ls8 ard
datacube  product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls8.odc-product.yaml

script_directory=$(dirname $(dirname "$(readlink -f "$0")"))
TEST_DATA=$(realpath "$script_directory/../tests/test_data/integration_tests")

if [[ $HOSTNAME == *"gadi"* ]]; then
  # Use a prod scene
  datacube  dataset add --confirm-ignore-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/097_075/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.odc-metadata.yaml
  datacube  dataset add --confirm-ignore-lineage /g/data/ka08/ga/l1c_metadata/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.odc-metadata.yaml
else
  # ls9 - The tar is from /g/data/da82/AODH/USGS/L1/Landsat/C2/097_075/LC90970752022239
  # moved to /g/data/u46/users/dsg547/test_data/c3/LC90970752022239/
  datacube  dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC90970752022239/LC09_L1TP_097075_20220827_20220827_02_T1.odc-metadata.yaml

  # S2 - don't know where the tar is
  # id:
  # datacube  dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-01/15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.odc-metadata.yaml
  # id: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
  datacube  dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.odc-metadata.yaml
fi

# Bug hunting
if [[ $HOSTNAME == *"gadi"* ]]; then
  # Use a prod scene
  datacube  dataset add --confirm-ignore-lineage /g/data/ka08/ga/l1c_metadata/2021/2021-03/30S140E-35S145E/S2A_MSIL1C_20210321T002051_N0209_R116_T55JBG_20210321T015608.odc-metadata.yaml
  datacube  dataset add --confirm-ignore-lineage /g/data/ka08/ga/l1c_metadata/2023/2023-12/30S125E-35S130E/S2B_MSIL1C_20231202T012709_N0509_R131_T52JCL_20231202T024607.odc-metadata.yaml

fi