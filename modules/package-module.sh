#!/usr/bin/env bash

set -eu

umask 002
#unset PYTHONPATH

echo "##########################"
echo
echo "module_dir = ${module_dir:=/g/data/v10/private/modules}"
echo "dea_module_dir = ${dea_module_dir:=/g/data/v10/public/modules}"
echo
echo "dea_module = ${dea_module:=dea/1.3.2}"
dea_module_name=${dea_module%/*}
instance=${dea_module_name##*-}
echo "instance = ${instance}"
echo
echo "eodatasets_head = ${eodatasets_head:=develop}"
echo "gqa_head = ${gqa_head:=develop}"
echo "gaip_head = ${gaip_head:=develop}"
echo
echo "##########################"
export module_dir dea_module

echoerr() { echo "$@" 1>&2; }

if [[ $# != 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <version>"
    exit 1
fi
export version="$1"

module use ${module_dir}/modulefiles
module use -a ${dea_module_dir}/modulefiles
module load ${dea_module}