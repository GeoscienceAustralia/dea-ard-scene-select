#!/usr/bin/env bash
# do a fresh login before running this to avoid issues from
# modules already loaded

#./package-module.sh "${2:-$(date +'%Y%m%d')}" # FOR PROD RUNS, enable this #### TODO - Gordon = uncomment this before PR
./package-module.sh  dev_${2:-$(date +'%Y%m%d')} #### TODO - Gordon = comment this before PR
