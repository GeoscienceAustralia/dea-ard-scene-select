#!/usr/bin/env bash

# start a local postgres
# sudo service postgresql start


if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20221025

  ODCCONF="--config ${USER}_dev.conf"
  BD="${USER}_dev"
  host=deadev.nci.org.au
  TEST_DATA="../test_data/ls9_reprocessing"
  
else
  echo "not NCI"
  ODCCONF="--config ${USER}_local.conf"
  # datacube -v  $ODCCONF system init
fi

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
    echo "duncans laptop"
    echo "conda activate odc2020"
fi



# clean up the database
psql -h $host $USER -d ${BD} -a -f db_delete_odc.sql


# Fill the database with scenes

# Defining landsat l1 metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_l1.odc-type.yaml

# ls9 l1
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls9.odc-product.yaml

# Defining landsat ard metadata
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/product_metadata/eo3_landsat_ard.odc-type.yaml 

# Defining ls9 ard
datacube $ODCCONF product add  https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/master/products/baseline_satellite_data/c3/ga_ls9c_ard_3.odc-product.yaml

# ---------------------
# Add two ARDs that are blocking two l1s
# ---------------------
# add and archive the l1 that produces the blocking ARD
# 4c68b81a-23a0-5e57-b983-96439fc4518c
datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml
datacube $ODCCONF dataset archive 4c68b81a-23a0-5e57-b983-96439fc4518c
#  Add the blocking ARD 3de6cb49-60da-4160-802b-65903dcbbac8
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml
# Add the l1 that is blocked by the ARD
# blocked l1 91e7489e-f05a-5b7e-a96c-f0f0549bdd34
datacube $ODCCONF dataset add --no-verify-lineage   /g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml

# ---------------------
# add and archive the l1 that produces the blocking ARD
# level1: d530018e-5dad-58c2-8471-15f17d506604
datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml
datacube $ODCCONF dataset archive d530018e-5dad-58c2-8471-15f17d506604
# Add the blocking ARD d9a499d1-1abd-4ed1-8411-d584ca45de25 level1: d530018e-5dad-58c2-8471-15f17d506604
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml


# Add the l1 that is blocked by the ARD
# blocked l1 17ebb0d1-5a43-5088-a833-5b19e540d891
datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml
# ---------------------
# Add a non-blocking ARD
# add the l1 level1:  a230aceb-528b-5895-a4d7-94226e172dcf
datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml

# Add the non-blocking ARD
# 43b726eb-77bd-42ac-bd11-ad1eea11863e
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml
