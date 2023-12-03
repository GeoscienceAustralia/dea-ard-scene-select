#!/bin/bash
#PBS -P u46
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=00:30:00,mem=155GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/fj7+scratch/fj7+scratch/u46+gdata/u46
#PBS -l ncpus=1

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
db_hostname="localhost"
if [[ $HOSTNAME == *"gadi"* ]]; then

   echo "gadi - NCI"
   module use /g/data/v10/public/modules/modulefiles
   module use /g/data/v10/private/modules/modulefiles
  if [ -d /g/data/u46/users/$USER/devmodules/modulefiles ]; then
    module use /g/data/u46/users/$USER/devmodules/modulefiles
  fi

  #module load dea/20221025
  # module load dea/20231123
  #module load h5-compression-filters/20230215

  # This is useful when testing a new ard-scene-select module
  # Comment out the export PYTHONPATH line below
  # module load ard-scene-select-py3-dea/20231010
  module load ard-scene-select-py3-dea/dev_20231130

  db_hostname="deadev.nci.org.au"
  TEST_DATA="/g/data/u46/users/dsg547/test_data"
  YAML_DIR=$TEST_DATA"/s2/autogen/yaml"
else
  # No yamls, or tars, so processing can't happen locally
  # Do this so DASS works
  YAML_DIR=$DIR
fi

# Use the local scene-select
SSPATH=$DIR/../../
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH
#export PYTHONPATH=$PYTHONPATH

export DATACUBE_DB_URL=postgresql://$USER"@"$db_hostname"/"$USER"_dev"

PRODUCTS='["usgs_ls9c_level1_2"]'
SCRATCH=$DIR"/scratch"
mkdir -p $SCRATCH

pkgdir=$SCRATCH/pkgdir$RANDOM
mkdir -p $pkgdir

PRODWAGLLS="${DIR}/../../scripts/prod/ard_env/prod-wagl-ls.env"
ENV_FILE="$DIR/index-test-odc.env"
# locally, did
# pip install --user -e dea-ard-scene-select
# python3 ../../scene_select/ard_scene_select.py  \
ard-scene-select \
   --workdir $SCRATCH \
   --pkgdir  $pkgdir \
   --logdir $SCRATCH  \
   --env $PRODWAGLLS  \
   --products $PRODUCTS \
   --project u46 \
   --walltime 05:00:00 \
   --index-datacube-env $ENV_FILE \
   --run-ard


PRODWAGLLS="${DIR}/../../scripts/prod/ard_env/prod-wagl-s2.env"
PRODUCTS='["esa_s2am_level1_0"]'
#python3 ../../scene_select/ard_scene_select.py  \
ard-scene-select \
   --workdir $SCRATCH \
   --pkgdir  $pkgdir \
   --logdir $SCRATCH  \
   --env $PRODWAGLLS  \
   --products $PRODUCTS \
   --project u46 \
   --walltime 05:00:00 \
   --yamls-dir $YAML_DIR \
   --index-datacube-env $ENV_FILE \
   --run-ard
