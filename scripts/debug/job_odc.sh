#!/bin/bash
#PBS -P u46
#PBS -q normal
#PBS -l walltime=0:05:00,mem=1GB
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load dea

./odc_search.py
