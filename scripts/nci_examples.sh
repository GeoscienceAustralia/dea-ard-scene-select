#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=00:30:00,mem=155GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load ard-scene-select-py3-dea/20200716

# local work dir 
#ard-scene-select --workdir scratch/

# local work dir and skipping odc
#ard-scene-select --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/


# local work dir 
# ard-scene-select --products '["ga_ls5t_level1_3"]' --workdir scratch/ 

# setting up a working ard call.  Should fail trying to connect to the db
ard-scene-select  --usgs-level1-files small_Landsat_Level1_Nci_Files.txt --workdir scratch/  --env prod-wagl.env  --index-datacube-env index-datacube.env --pkgdir  scratch/ --logdir scratch/ --project u46 --walltime 05:00:00 #--run-ard

# need access to v10 for this to work
# ard-scene-select --workdir /g/data/v10/projects/landsat_c3/wagl_workdir  --pkgdir  /g/data/xu18/ga --logdir /g/data/v10/projects/landsat_c3/wagl_logdir --env /g/data/v10/Landsat-Collection-3-ops/OFFICIAL/Collection-3_5.4.1.env --project v10 --walltime 05:00:00 #--run-ard
