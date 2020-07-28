#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles
module load dea/20190329
module load wagl/5.4.1

SSPATH=$PWD/../../

# so it uses the dev scene select
#echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH

# production example
#python3 ard_scene_select.py --products '["ga_ls5t_level1_3"]' --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

#  local work dir
python3 ../../scene_select/ard_scene_select.py  --products '["ga_ls5t_level1_3"]' --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project u46 --walltime 05:00:00 #--run-ard

# local work dir and skipping odc
#python3 ../../scene_select/ard_scene_select.py --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/  --env prod-wagl.env  --index-datacube-env c3-samples-index-datacube.env --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 05:00:00 #--run-ard

# local work dir and skipping odc
#ard-scene-select --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --project v10 --walltime 05:00:00 #--run-ard

# skipping odc
#python3 ard_scene_select.py  --usgs-level1-files small_Landsat_Level1_Nci_Files.txt

# using Australian_Wrs_list.txt
#python3 ard_scene_select.py  --usgs-level1-files data/All_Landsat_Level1_Nci_Files.txt  --workdir scratch/ --allowed-codes data/Australian_Wrs_list.txt

# testing example
#python3 ard_scene_select.py --products '["ga_ls5t_level1_3"]' --workdir $PWD  --pkgdir $PWD --logdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard
