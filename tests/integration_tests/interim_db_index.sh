#!/usr/bin/env bash

# start a local postgres
# sudo service postgresql start


if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea
  #module load ard-pipeline/devv2.1

  TEST_DATA=/g/data/u46/users/dsg547/test_data
  ODCCONF="--config dsg547_dev.conf"
  
else
  echo "not NCI"
  ODCCONF="--config dsg547_local.conf"
  TEST_DATA=$HOME/test_data
  # datacube -v  $ODCCONF system init
fi

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
    echo "duncans laptop"
    echo "conda activate odc2020"
fi


# Defining S2 l1's
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel.odc-type.yaml
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/esa_s2am_level1_0.odc-product.yaml
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/esa_s2bm_level1_0.odc-product.yaml

# Defining S2 ard's
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_sentinel_ard.odc-type.yaml

# Bad check-in @ head
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/0911aaf770658e7aea41757b1b4801c9cdb5bdc0/products/baseline_satellite_data/c3/ga_s2am_ard_3.odc-product.yaml
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/0911aaf770658e7aea41757b1b4801c9cdb5bdc0/products/baseline_satellite_data/c3/ga_s2bm_ard_3.odc-product.yaml

# Defining landsat l1 metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_l1.odc-type.yaml

# Defining ls7 c1
datacube --config dsg547_dev.conf product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls7.odc-product.yaml


# Defining ls8 l1
# c1
datacube --config dsg547_dev.conf product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8.odc-product.yaml

# c2 # usgs_ls8c_level1_2
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8_c2.odc-product.yaml

# Defining ls8 ard
datacube --config dsg547_dev.conf metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube --config dsg547_dev.conf product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls8.odc-product.yaml

# Defining ls7 ard
datacube --config dsg547_dev.conf product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls7.odc-product.yaml

# ---------------------
# ---------------------
#R2.6 Filter out ls8 l1 scenes if the dataset has a child, the child is interim and there is no ancillary 
#
# Add an L1 scene
# id: 768675cd-0c2b-5a17-871a-1f35eabac78e
datacube --config dsg547_dev.conf dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC80960702022336_do_interim/LC08_L1TP_096070_20221202_20221212_02_T1.odc-metadata.yaml

# Add the ARD of the scene
# id:  8def2298-4db4-42f8-aa6d-1465d301ff24
# lineage:  level1: - 768675cd-0c2b-5a17-871a-1f35eabac78e
datacube --config dsg547_dev.conf dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC80960702022336_ard/ga_ls8c_ard_3-2-1_096070_2022-12-02_interim.odc-metadata.yaml

# Filter Outcome- scene not selected to process - ls_go_select
# ---------------------


# ---------------------
#R2.6 Filter out S2 l1 scenes if the dataset has a child, the child is interim and there is no ancillary 
#
# Add an L1 scene
# id: 6a446ae9-7b10-544f-837b-c55b65ec7d68
datacube --config dsg547_dev.conf dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-12/30S130E-35S135E/S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408.odc-metadata.yaml

# Add the ARD of the scene
# id:  3d80e4ef-10a1-4e7c-bac1-4691871047ae
# lineage:  level1: -  6a446ae9-7b10-544f-837b-c55b65ec7d68
datacube --config dsg547_dev.conf dataset add --confirm-ignore-lineage $TEST_DATA/c3/S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408_ard/ga_s2am_ard_3-2-1_53HMC_2022-12-03_interim.odc-metadata.yaml

# Filter Outcome- scene not selected to process - s2_go_select
# ---------------------
# ---------------------

# ---------------------
# R3.2 Process an S2  scene if the child is interim and ancill data is there (Archive the interim ARD, process to final)
# Add the L1 scene of interim ARD
# id: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
datacube --config dsg547_dev.conf dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.odc-metadata.yaml

# The ARD results from an ARD dataset modded to be interim and the UUID is changed
# dea:dataset_maturity: interim
# id:  e4dc2251-275d-4c72-a89c-ec1a5a080eee
#lineage:  level1: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
datacube --config dsg547_dev.conf dataset add  $TEST_DATA/c3/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T02193_ard/ga_s2am_ard_3-2-1_53JMG_2022-11-23_final.odc-metadata.yaml

# Filter Outcome- scene selected to process - s2_go_select
# /g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.zip

# ARD selected for archiving uuid_to_archive.txt
# e4dc2251-275d-4c72-a89c-ec1a5a080eee

# Batch outcome -  ls_go_select  with ARD processing
# s2 ARD produced for 2022-11-23
# region_code:  53JMG
# ---------------------
./check_db.sh
