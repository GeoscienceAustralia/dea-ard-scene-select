#!/bin/bash

# region code needs datacube.
#source /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env
module use /g/data/v10/public/modules/modulefiles
module load dea

# prod example
#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

# testing example
python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir $PWD  --pkgdir $PWD --logdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard

#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir $PWD  --pkgdir $PWD --logdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --nodes 1 --project v10 --walltime 00:10:00 --test --run-ard

#python3 region_code_filter.py --products '[]' --workdir $PWD --logdir $PWD --pkgdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --nodes 1 --project v10 --walltime 00:10:00 --test


#python3 region_code_filter.py --products '["ga_ls5t_level1_3"]' --workdir $PWD --logdir $PWD --pkgdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --nodes 1 --project v10 --walltime 00:10:00 --test
#python3 region_code_filter.py --workdir $PWD --logdir $PWD --pkgdir $PWD --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --nodes 1 --project v10 --walltime 05:00:00
