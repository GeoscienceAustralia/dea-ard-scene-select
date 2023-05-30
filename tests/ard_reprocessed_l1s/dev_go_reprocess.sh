#!/bin/bash
# If a number is passed in it is assumed to be the scene limit
# otherwise the default is 400

set -o errexit
set -o xtrace

if [[ "$HOSTNAME" == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles

	module load ard-scene-select-py3-dea/20230525
>>>>>>> 16adde5 (update modules, remove commented code)

fi

if [ -z "$1" ]
  then
    echo "No argument supplied"
	scene_limit_value=400
else
	scene_limit_value=$1
fi

dry_run=" "
run_ard="--run-ard"
ard_env="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"
index_arg="--index-datacube-env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/index-datacube.env"
base_path="/g/data/xu18/ga/"
new_base_path="/g/data/xu18/ga/reprocessing_staged_for_removal"

project="v10"
pkgdir="/g/data/xu18/ga"
date=$(date '+%Y%m%dT%H%M%S')
basedir="/g/data/v10/work/ls_c3_ard/"

# #/* The sed command below will remove this block of test code
# and generate the production script called go_reprocess.sh
# sed '/#\/\*/,/#\*\// d' dev_go_reprocess.sh > go_reprocess.sh
# sed '/#\/\*/,/#\*\// d' landsat_c3/dev_go_reprocess.sh > landsat_c3/go_reprocess.sh

echo "Do ./db_index.sh first, to initialise the dev ODC"
project="u46"

# Write to this directory for log dir and work dir,
# by modifying the basedir.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
basedir="$DIR"/scratch
# pkgdir needs to be writable
pkgdir=$basedir/pkgdir$RANDOM

# To set up the dev ODC do ./db_index.sh 

# a dev run uses the dev database for scene select
# a prod run uses the prod database for scene select
# run ['dev'|'prod']
run='dev'
#run='prod'
if [ "$run" = "prod" ]; then
   	# A dry run is necessary to avoid trying to move production ARD.
	dry_run="--dry-run"
	run_ard=""
else
   	# This will use the dev database when calling scene select
   	# and indexing the ARD.

	run_ard="--run-ard"

   	# Run the local scene select
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
logdir="$basedir/logdir/${date}_reprocess"
workdir="$basedir/workdir/${date}_reprocess"

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
--scene-limit $scene_limit_value \
$dry_run \
$run_ard \
$index_arg
