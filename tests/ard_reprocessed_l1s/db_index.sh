#!/usr/bin/env bash

# Note if running on NCI the l1 production data is used for the blocked l1s
# so ARD processing can be tested

# start a local postgres
# sudo service postgresql start

# I don't like this, but it is needed to get the correct datacube env
# for the second time db_index is used.
export -n DATACUBE_CONFIG_PATH
export -n DATACUBE_ENVIRONMENT

ODCDB="${USER}_dev"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEST_DATA_REL="${SCRIPT_DIR}/../test_data/ls9_reprocessing"
TEST_DATA=$(realpath "$TEST_DATA_REL")

if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  module use /g/data/v10/public/modules/modulefiles
  module use /g/data/v10/private/modules/modulefiles

  module load dea/20221025

  ODCCONF="--config ${SCRIPT_DIR}/${USER}_dev.conf"
  host=deadev.nci.org.au
else
  echo "not NCI"
  ODCCONF=""
  host=localhost

  # datacube -v  $ODCCONF system init
fi

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
  echo "duncans laptop"
  echo "conda activate /home/duncan/bin/miniconda3/envs/odc2020"
  echo "sudo service postgresql start"
  echo "This env does not have hd5, so do not do ARD processing"
  ODCCONF="--config ${SCRIPT_DIR}/${USER}_local.conf"
  ODCDB="${USER}_local"
  #export DATACUBE_ENVIRONMENT="$ODCDB"_local
fi


SSPATH=$DIR/../..

# so it uses the dev scene select
echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
echo $PYTHONPATH


# delete and recreate the file structure
rm -rf $TEST_DATA/moved/ga_ls9c_ard_3/
rm -rf $TEST_DATA/ga_ls9c_ard_3/
cp -r $TEST_DATA/a_ga_ls9c_ard_3_raw/ $TEST_DATA/ga_ls9c_ard_3/
mkdir -p $TEST_DATA/moved/

# clean up the database
psql -h $host $USER -d ${ODCDB} -a -f ${SCRIPT_DIR}/db_delete_odc.sql

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
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220621_02_T1.odc-metadata.yaml
datacube $ODCCONF dataset archive 4c68b81a-23a0-5e57-b983-96439fc4518c

#  Add the blocking ARD 3de6cb49-60da-4160-802b-65903dcbbac8
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/092/081/2022/06/21/ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml
# Add the l1 that is blocked by the ARD
# blocked l1 91e7489e-f05a-5b7e-a96c-f0f0549bdd34
if [[ $HOSTNAME == *"gadi"* ]]; then
    datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml
else
    datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/l1_Landsat_C2/092_081/LC90920812022172/LC09_L1TP_092081_20220621_20220802_02_T1.odc-metadata.yaml
fi

# ---------------------
# add and archive the l1 that produces the blocking ARD
# level1: d530018e-5dad-58c2-8471-15f17d506604
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220627_02_T1.odc-metadata.yaml
datacube $ODCCONF dataset archive d530018e-5dad-58c2-8471-15f17d506604
# Add the blocking ARD d9a499d1-1abd-4ed1-8411-d584ca45de25 level1: d530018e-5dad-58c2-8471-15f17d506604
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/102/076/2022/06/27/ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml


# Add the l1 that is blocked by the ARD
# blocked l1 17ebb0d1-5a43-5088-a833-5b19e540d891
if [[ $HOSTNAME == *"gadi"* ]]; then
    datacube $ODCCONF dataset add --no-verify-lineage /g/data/da82/AODH/USGS/L1/Landsat/C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml
else
    datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/l1_Landsat_C2/102_076/LC91020762022178/LC09_L1TP_102076_20220627_20220802_02_T1.odc-metadata.yaml
fi
# ---------------------
# Add a non-blocking ARD
# add the l1 level1:  a230aceb-528b-5895-a4d7-94226e172dcf
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/l1_Landsat_C2/095_074/LC90950742022177/LC09_L1TP_095074_20220626_20220802_02_T1.odc-metadata.yaml

# Add the non-blocking ARD
# 43b726eb-77bd-42ac-bd11-ad1eea11863e
datacube $ODCCONF dataset add --no-verify-lineage $TEST_DATA/ga_ls9c_ard_3/095/074/2022/06/26/ga_ls9c_ard_3-2-1_095074_2022-06-26_final.odc-metadata.yaml


# SSPATH=$PWD/../..

# # so it uses the dev scene select
# echo $PYTHONPATH
# [[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

# Doing this at the start messes with  $ODCCONF
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export DATACUBE_CONFIG_PATH=$DIR/datacube.conf
export DATACUBE_ENVIRONMENT=$ODCDB
mkdir -p $DIR/scratch/   # for test logs
