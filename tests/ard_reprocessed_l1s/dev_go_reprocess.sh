#!/bin/bash
# Do ls9 reprocessing

#PBS -P v10
#PBS -W umask=017
#PBS -q express
#PBS -l walltime=02:00:00,mem=155GB,other=pernodejobfs
#PBS -l wd
#PBS -l storage=gdata/v10+scratch/v10+gdata/if87+gdata/xu18+scratch/xu18+scratch/u46+gdata/u46
#PBS -l ncpus=1

set -o errexit
set -o xtrace

if [[ "$HOSTNAME" == *"gadi"* ]]; then
	echo If module load breaks check out a clean environment
	module use /g/data/v10/public/modules/modulefiles
	module use /g/data/v10/private/modules/modulefiles

	module load ard-scene-select-py3-dea/20230616

fi

scene_limit_value=1000
dry_run=" "
run_ard="--run-ard"
ard_env="/g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/prod-wagl-ls.env"
index_arg="--index-datacube-env /g/data/v10/projects/c3_ard/dea-ard-scene-select/scripts/prod/ard_env/index-datacube.env"
ard_path="/g/data/xu18/ga/"
new_ard_path="/g/data/xu18/ga/reprocessing_staged_for_removal"
product="ga_ls9c_ard_3"

project="v10"
pkgdir="/g/data/xu18/ga"
date=$(date '+%Y%m%dT%H%M%S')
basedir="/g/data/v10/work/ls_c3_ard"

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
	scene_limit_value=1000000
	index_arg=""
	product="ga_ls8c_ard_3"
else
   	# This will use the dev database when calling scene select
	# and index the ARD.

   	# Run the local scene select
	# This is so indexing the ARD uses the dev database
	dev_index_env="${DIR}/index-test-odc.env"

	# This is so scene select uses the dev database
	export DATACUBE_CONFIG_PATH="${DIR}/datacube.conf"
	export DATACUBE_ENVIRONMENT="${USER}_dev"
	
	index_arg="--index-datacube-env $dev_index_env"
	test_data_rel="${DIR}/../test_data/ls9_reprocessing"
	test_data=$(realpath "$test_data_rel")
	ard_path=$test_data
	new_ard_path="$test_data/moved/"

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
--product "$product" \
--current-base-path $ard_path \
--new-base-path $new_ard_path \
--scene-limit $scene_limit_value \
$dry_run \
$run_ard \
$index_arg
