#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module load dea

# production example
#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

#  local work dir
python3 region_code_filter.py --workdir scratch/  --pkgdir  scratch/ --logdir scratch/ --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 --landsat-AOI #--run-ard

# skipping odc
#python3 region_code_filter.py  --usgs-level1-files data/small_Landsat_Level1_Nci_Files.txt

# using Australian_Wrs_list.txt
#python3 region_code_filter.py  --usgs-level1-files data/All_Landsat_Level1_Nci_Files.txt  --workdir scratch/ --allowed-codes data/Australian_Wrs_list.txt

# using Landsat AOI
python3 region_code_filter.py  --usgs-level1-files data/All_Landsat_Level1_Nci_Files.txt  --workdir scratch/ --landsat-AOI

# testing example
#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir $PWD  --pkgdir $PWD --logdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard
