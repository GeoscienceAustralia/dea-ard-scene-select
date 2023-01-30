#!/bin/bash

if [[ $HOSTNAME == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
	module use /g/data/u46/users/dsg547/devmodules/modulefiles


	#module load ard-scene-select-py3-dea/20211115
	#module load ard-scene-select-py3-dea/20220121
	module load ard-scene-select-py3-dea/20220516
	#module load ard-scene-select-py3-dea/20220922

	#module load h5-compression-filters/20200612
fi

mkdir -p scratch

SSPATH=$PWD/../../

#PRODUCTS = '["ga_ls5t_level1_3", "ga_ls7e_level1_3", \
#     "usgs_ls5t_level1_1", "usgs_ls7e_level1_1", "usgs_ls8c_level1_1"]'

# You'll need a scratch directory
# mkdir scratch

# so it uses the dev scene select
# echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
# echo $PYTHONPATH

# The module
#time ard-scene-select --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env  --products '["usgs_ls7e_level1_1"]' --scene-limit 999999 #--find-blocked

# production example
#python3 ard_scene_select.py --products '["ga_ls5t_level1_3"]' --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

#  local code local work dir, one product
# slow
ard_loc='/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env'
#ard_loc='/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env'

time python3 ../../scene_select/ard_scene_select.py --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env $ard_loc  --products '["usgs_ls9c_level1_2"]'  --find-blocked # --scene-limit 9999


ard_loc='/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env'
ard_loc_prod='/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env'
ard_loc_dev='/g/data/u46/users/dsg547/sandbox/dea-ard-scene-select/scripts/examples/prod-wagl.env'

# ["esa_s2am_level1_0", "esa_s2bm_level1_0"]
# if a dont run-ard I dont need to give the yamls..
#time python3 ../../scene_select/ard_scene_select.py --workdir scratch/ --pkgdir scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env $ard_loc_dev --products '["esa_s2am_level1_0", "esa_s2bm_level1_0"]'  --find-blocked  #--run-ard  --scene-limit 3  --find-blocked

# PRODUCTS= '["usgs_ls8c_level1_1", "usgs_ls7e_level1_1", \
    #    "usgs_ls7e_level1_2", "usgs_ls8c_level1_2"]'

#PRODUCTS='["usgs_ls8c_level1_1", "usgs_ls7e_level1_1", "usgs_ls7e_level1_2", "usgs_ls8c_level1_2"]'


#time ard-scene-select --workdir scratch/ --pkgdir scratch/ --logdir scratch/ --project u46 --walltime 10:00:00  --env $ard_loc_prod --find-blocked --products '["usgs_ls9c_level1_2"]' # --products '[ "usgs_ls7e_level1_2", "usgs_ls8c_level1_2"]'  #--scene-limit 1 --run-ard #--find-blocked

#time python3 ../../scene_select/ard_scene_select.py --workdir scratch/  --pkgdir  scratch/ --logdir scratch/    --project u46 --walltime 05:00:00  --products '["usgs_ls8c_level1_2"]' --find-blocked #--run-ard  --env $ard_loc #--env /g/data/v10/projects/c3_ard/dea-ard/prod/ard_env/prod-wagl.env --index-datacube-env $PWD/c3-samples-index-datacube.env

#time python3 ../../scene_select/ard_scene_select.py --products '["usgs_ls7e_level1_1"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/   --index-datacube-env $PWD/c3-samples-index-datacube.env --project u46 --walltime 10:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env #--find-blocked #--run-ard  # --products '["usgs_ls8c_level1_1"]' --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env


#time ard-scene-select --workdir scratch/  --pkgdir  scratch/ --logdir scratch/  --index-datacube-env $PWD/c3-samples-index-datacube.env --project u46 --walltime 10:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env  --scene-limit 999999 #--find-blocked

#time python3 ../../scene_select/ard_scene_select.py --products '["ga_ls5t_level1_3"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/   --index-datacube-env $PWD/c3-samples-index-datacube.env --project u46 --walltime 7:00:00  --env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env

# --products '["ga_ls5t_level1_3"]'

# This will use the dev ard scene select too..., all products
# unless the PYTHONPATH line above is commented out
# slow
#time ard-scene-select  --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 02:30:00 --scene-limit 999999   #--days-to-exclude '["2020-08-09:2020-09-03"]'

#  local code local work dir ls5t
# use ODC, fast for OEC
#time python3 ../../scene_select/ard_scene_select.py  --products '["ga_ls5t_level1_3"]' --days-to-exclude '["2020-08-09:2020-09-03"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 05:00:00 #--stop-logging #--run-ard
#time python3 ../../scene_select/ard_scene_select.py  --products '["ga_ls5t_level1_3"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 05:00:00 #--stop-logging #--run-ard

#  local code local work dir ls8c
# use ODC, fast for OEC#
#time python3 ../../scene_select/ard_scene_select.py  --products '["usgs_ls8c_level1_1"]'  --logdir scratch/ #--workdir scratch/  --pkgdir  scratch/ #--env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 05:00:00 #--stop-logging #--run-ard

#  local code local work dir #
#python3 ../../scene_select/ard_scene_select.py  --products '["usgs_ls8c_level1_1"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 05:00:00 #--run-ard

# local code local work dir and skipping odc
#python3 ../../scene_select/ard_scene_select.py --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/ --index-datacube-env c3-samples-index-datacube.env --pkgdir scratch/ --logdir scratch/ --project u46 --walltime 05:00:00 #--run-ard  --env prod-wagl.env

# prod module local work dir and skipping odc
#ard-scene-select --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project v10 --walltime 05:00:00 #--run-ard

# skipping odc
#python3 ard_scene_select.py  --usgs-level1-files small_Landsat_Level1_Nci_Files.txt

# using Australian_Wrs_list.txt
#python3 ../../scene_select/ard_scene_select.py  --usgs-level1-files ../../tests/test_data/All_Landsat_Level1_Nci_Files.txt  --workdir scratch/ --allowed-codes ../../tests/test_data/standard_wrs_list.txt  --logdir scratch/
