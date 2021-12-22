#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=00:10:00,mem=6GB,ncpus=1,jobfs=2GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=scratch/v10+gdata/v10+scratch/da82+gdata/da82+scratch/u46+gdata/u46

#source /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl.env

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

#module load ard-pipeline/20211125  # This fails
#module load ard-pipeline/devv2.1   # OK
module load ard-pipeline/20211222   # OK

ard_pbs --help
#python3 -c "print(5)"
