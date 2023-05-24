#!/bin/bash
# If a number is passed in it is assumed to be the scene limit
# otherwise the default is 400

set -o errexit
set -o xtrace

if [[ "$HOSTNAME" == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles
  	
	# remove for the production script
	module use /g/data/u46/users/$USER/devmodules/modulefiles

	# Update for the production script
	module load ard-scene-select-py3-dea/dev_20230524
	# module load ard-scene-select-py3-dea/20230522

fi

if [ -z "$1" ]
  then
    echo "No argument supplied"
	scenelimitvalue=400
else
	scenelimitvalue=$1
fi

dry_run=" "
runard="--run-ard"
ard_env="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"
index_arg="--index-datacube-env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/index-datacube.env"
base_path="/g/data/xu18/ga/"
new_base_path="/g/data/xu18/ga/reprocessing_staged_for_removal"

scenelimit="--scene-limit $scenelimitvalue"
project="v10"
pkgdir="/g/data/xu18/ga"
date=$(date '+%Y%m%d_%H%M%S')
basedir="/g/data/v10/work/ls_c3_ard/"

# #/* The sed command below will remove this block of test code
# and generate the production script called go_reprocess.sh
# sed '/#\/\*/,/#\*\// d' dev_go_reprocess.sh > go_reprocess.sh
# sed '/#\/\*/,/#\*\// d' landsat_c3/dev_go_reprocess.sh > landsat_c3/go_reprocess.sh

project="u46"

# To set up the dev ODC do ./db_index.sh 
# run ['dev'|'prod']
run='dev'
#run='prod'
if [ "$run" = "prod" ]; then
   	# This will use the production database when calling scene select
   	# A dry run is necessary to avoid trying to move production ARD.
	dry_run="--dry-run"
	runard=""
else
   	# This will use the dev database when calling scene select
   	# and indexing the ARD.

	runard="--run-ard"

   	# Run the local scene select
	DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

	basedir="$DIR"/scratch
	pkgdir=$basedir/pkgdir$RANDOM

	# This is so indexing the ARD uses the dev database
	dev_index_env="${DIR}/index-test-odc.env"

	# This is so scene select uses the dev database
	export DATACUBE_CONFIG_PATH="${DIR}/datacube.conf"
	export DATACUBE_ENVIRONMENT="${USER}_dev"
	
	index_arg="--index-datacube-env $dev_index_env"
	test_data_rel="${DIR}/../test_data/ls9_reprocessing"
	test_data=$(realpath "$test_data_rel")
	base_path=$test_data
	new_base_path="$test_data/moved/"

   # Need more info. It has to be just like an airflow prod run
   #index="--index "
fi


mkdir -p "$pkgdir"
# #*/ The end of the sed removed block of code
logdir="$basedir/logdir/$date"
workdir="$basedir/workdir/$date"

mkdir -p "$logdir"
mkdir -p "$workdir"


# ard-reprocessed-l1s module
ard-reprocessed-l1s --walltime 10:00:00 \
--pkgdir  "$pkgdir" \
--logdir "$logdir"  \
--workdir "$workdir" \
--project "$project"  \
--env "$ard_env"  \
--current-base-path $base_path \
--new-base-path $new_base_path \
$scenelimit $dryrun $runard $index_arg
