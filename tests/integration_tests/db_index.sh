#!/usr/bin/env bash

# start a local postgres
# sudo service postgresql start


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
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls7.odc-product.yaml

# Defining ls8 l1
# c1
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8.odc-product.yaml

# c2
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/l1_ls8_c2.odc-product.yaml

# Defining ls8 ard
datacube $ODCCONF metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls8.odc-product.yaml

# Defining ls7 ard
datacube $ODCCONF product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/products/ard_ls7.odc-product.yaml

# ---------------------
# R1.1 for s2: Unfiltered scenes are ARD processed
# R1.2: S2 scenes can be processed when the zip and yaml are in different directories
# add an S2 l1
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/s2/autogen/yaml/2022/2022-01/15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.odc-metadata.yaml

# Filter Outcome - s2_go_select
# $TEST_DATA/c3/s2_autogen/yaml/15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip selected for processing

# Batch outcome - s2_go_select with ARD processing
# S2 ARD produced for 2022-01-24
# region_code: 54LYH

# ---------------------

# ---------------------
# R1.1 for ls: Unfiltered scenes are ARD processed
# The tar is from /g/data/da82/AODH/USGS/L1/Landsat/C1/092_085/LC80920852020223
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC80920852020223_good/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml

# Filter Outcome - ls_go_select
# $TEST_DATA/c3/LC80920852020223_good/LC08_L1TP_092085_20200810_20200821_01_T1.tar

# Batch outcome - s2_go_select with ARD processing
# ls ARD produced for 2020-08-10
# region_code: 092085

# ---------------------

# ---------------------
#R2.2 Filter out scenes that do not match the product pattern
datacube $ODCCONF dataset add  $TEST_DATA/c3/LE71080732020343_level_too_low/LE07_L1GT_108073_20201208_20210103_01_T2.odc-metadata.yaml

# Filter Outcome- scene not selected to process - ls_go_select
# ---------------------

# ---------------------
# 2.3 Filter out if outside the AOI
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC80920852020223_OUTAOI/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml
# the region has been edited to 999999 :)

# ---------------------

# ---------------------
# 2.4 Filter out if no ancillary
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC80920852020223_no_ancillary/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml
# the datetime has been edited to 2030 :)

# ---------------------

# ---------------------
# 2.5 Filter within the excluded days
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC80920852020223_excluded_day/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml
# the datetime is 2009-01-04 05:15:01.883365Z

# ---------------------
# ---------------------
#R2.6 Filter out l1 scenes if the dataset has a child and the child is not archived 
#
# Add an L1 scene
# id: 91f2fbd8-8ad5-550b-a62c-d819e1a4baaa
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC80900742020304/LC08_L1GT_090074_20201030_20201106_01_T2.odc-metadata.yaml

# Add the ARD of the scene
# id: 30fe6bd3-7cd5-488f-9a06-313220489bdd
# lineage:  level1: - 91f2fbd8-8ad5-550b-a62c-d819e1a4baaa
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/ARD_LC80900742020304_final/ga_ls8c_ard_3-1-0_090074_2020-10-30_final.odc-metadata.yaml

# Filter Outcome- scene not selected to process - ls_go_select
# ---------------------


# ---------------------
# ---------------------
#R2.6 Filter out ls8 l1 scenes if the dataset has a child, the child is interim and there is no ancillary 
#
# Add an L1 scene
# id: 768675cd-0c2b-5a17-871a-1f35eabac78e
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC80960702022336_do_interim/LC08_L1TP_096070_20221202_20221212_02_T1.odc-metadata.yaml

# Add the ARD of the scene
# id:  8def2298-4db4-42f8-aa6d-1465d301ff24
# lineage:  level1: - 768675cd-0c2b-5a17-871a-1f35eabac78e
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC80960702022336_ard/ga_ls8c_ard_3-2-1_096070_2022-12-02_interim.odc-metadata.yaml

# Filter Outcome- scene not selected to process - ls_go_select
# ---------------------
# ---------------------
#R2.6 Filter out S2 l1 scenes if the dataset has a child, the child is interim and there is no ancillary 
#
# Add an L1 scene
# id: 6a446ae9-7b10-544f-837b-c55b65ec7d68
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-12/30S130E-35S135E/S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408.odc-metadata.yaml

# Add the ARD of the scene
# id:  3d80e4ef-10a1-4e7c-bac1-4691871047ae
# lineage:  level1: -  6a446ae9-7b10-544f-837b-c55b65ec7d68
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/S2A_MSIL1C_20221203T005711_N0400_R002_T53HMC_20221203T022408_ard/ga_s2am_ard_3-2-1_53HMC_2022-12-03_interim.odc-metadata.yaml

# Filter Outcome- scene not selected to process - s2_go_select
# ---------------------

# ---------------------
# 3.1 Process a scene if the ancillary is not there, after the wait time (Process to interim)
datacube $ODCCONF dataset add --confirm-ignore-lineage $TEST_DATA/c3/LC80920852020223_do_interim/LC08_L1TP_092085_20200810_20200821_01_T1.odc-metadata.yaml
# the datetime is 1944-08-10

# Filter Outcome - ls_go_select
# $TEST_DATA/c3/LC80920852020223_do_interim/LC08_L1TP_092085_20200810_20200821_01_T1.tar

# Batch outcome - s2_go_select with ARD processing
# ls ARD produced for 2020-08-10
# region_code: 092085

# ---------------------

# ---------------------
# R3.2 Process a scene if the child is interim and ancill data is there (Archive the interim ARD, process to final)
# use this AOI file so this scene is in area Australian_AOI_107069_added.json
# Add the L1 scene of interim ARD
# id: eaf3ef69-f813-59f7-836a-7cb0ed888d96
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC81070692020200/LC08_L1GT_107069_20200718_20200722_01_T2.odc-metadata.yaml

# The ARD results from an ARD dataset modded to be interim and the UUID is changed
# id: e987923c-090f-4ac3-9688-5cadcccaacad
#lineage:  level1: eaf3ef69-f813-59f7-836a-7cb0ed888d96
datacube $ODCCONF dataset add  $TEST_DATA/c3/ARD_interim_LC81070692020200/ga_ls8c_ard_3-1-0_107069_2020-07-18_final.odc-metadata.yaml

# Filter Outcome- scene selected to process - ls_go_select
# $TEST_DATA/c3/LC81070692020200/LC08_L1GT_107069_20200718_20200722_01_T2.tar

# ARD selected for archiving uuid_to_archive.txt
# e987923c-090f-4ac3-9688-5cadcccaacad

# Batch outcome -  ls_go_select  with ARD processing
# ls ARD produced for 2020-07-18
# region_code: 107069
# ---------------------
# ---------------------
# R3.2 Process an S2  scene if the child is interim and ancill data is there (Archive the interim ARD, process to final)
# Add the L1 scene of interim ARD
# id: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/s2/autogen/yaml/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.odc-metadata.yaml

# The ARD results from an ARD dataset modded to be interim and the UUID is changed
# dea:dataset_maturity: interim
# id:  e4dc2251-275d-4c72-a89c-ec1a5a080eee
#lineage:  level1: df4a46b0-258c-5d51-b48e-aeda4dd7de4e
datacube $ODCCONF dataset add  $TEST_DATA/c3/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T02193_ard/ga_s2am_ard_3-2-1_53JMG_2022-11-23_final.odc-metadata.yaml

# Filter Outcome- scene selected to process - s2_go_select
# /g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.zip

# ARD selected for archiving uuid_to_archive.txt
# e4dc2251-275d-4c72-a89c-ec1a5a080eee

# Batch outcome -  ls_go_select  with ARD processing
# s2 ARD produced for 2022-11-23
# region_code:  53JMG
# ---------------------
# ---------------------
# 3.3 Filter out the l1 scene if there is already an ARD scene with the same scene_id, that is un-archived.
# Set up for testing a reprocessed l1 scene

# Add old L1 scene.
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC81150802019349/LC08_L1TP_115080_20191215_20191226_01_T1.odc-metadata.yaml

# Add the new L1 scene
# 67aade4b-9553-561c-923f-18bd8ff050f1
datacube $ODCCONF dataset add --confirm-ignore-lineage  $TEST_DATA/c3/LC81150802019349/LC08_L1TP_115080_20191215_20201023_01_T1.odc-metadata.yaml

# Add the ARD of the old L1 scene
# id: fa083e38-a753-4a30-82f7-9deb27ee1602
# lineage: level1: - 760315b3-e147-5db2-bb7f-0e52efd4453d
datacube $ODCCONF dataset add  $TEST_DATA/c3/ARD_LC81150802019349_old/ga_ls8c_ard_3-1-0_115080_2019-12-15_final.odc-metadata.yaml

# Archive the l1 where there is reprocessed data
# Since this is what happens with the downloader
datacube $ODCCONF dataset archive 760315b3-e147-5db2-bb7f-0e52efd4453d

# Filter Outcome - ls_go_select
# The scene is filtered out since there is alread a child.

# ---------------------


datacube  $ODCCONF product list # 
./check_db.sh

# overall filter outcome for  s2_go_select.sh - see above, for 1.1 and 1.2 and 3.2.
# /g/data/u46/users/${USER}/test_data/c3/s2_autogen/zip/15S140E-20S145E/S2A_MSIL1C_20220124T004711_N0301_R102_T54LYH_20220124T021536.zip
#/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2022/2022-11/30S130E-35S135E/S2A_MSIL1C_20221123T005711_N0400_R002_T53JMG_20221123T021932.zip

# overall uuid_to_archive.txt for s2
# e4dc2251-275d-4c72-a89c-ec1a5a080eee

# overall filter outcome for  ls_go_select.sh - see above, for 3.2 and 1.1 ls
# $TEST_DATA/c3/LC80920852020223_good/LC08_L1TP_092085_20200810_20200821_01_T1.tar
# $TEST_DATA/c3/LC81070692020200/LC08_L1GT_107069_20200718_20200722_01_T2.tar
# $TEST_DATA/c3/LC80920852020223_do_interim/LC08_L1TP_092085_20200810_20200821_01_T1.tar

# overall uuid_to_archive.txt for landsat
# e987923c-090f-4ac3-9688-5cadcccaacad
