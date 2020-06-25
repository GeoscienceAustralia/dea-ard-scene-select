#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module load dea

# production example
#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

# skipping odc
python3 region_code_filter.py  --usgs-level1-files data/small_Landsat_Level1_Nci_Files.txt --workdir  scratch/ --pkgdir scratch/ --logdir scratch/

# testing example
#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir $PWD  --pkgdir $PWD --logdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard
