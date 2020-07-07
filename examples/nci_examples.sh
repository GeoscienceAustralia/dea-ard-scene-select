#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20200707

# local work dir and skipping odc
ard-scene-select --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/

# local work dir 
ard-scene-select --products '["ga_ls5t_level1_3"]' --workdir scratch/ 
