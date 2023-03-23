#!/usr/bin/env bash
# do a fresh login before running this to avoid issues from
# modules already loaded

# This script, by default, would run in the non production
# environment. If a production run is required,
# a --prod flag is to be provided to this script.
if [[ $1 == "--prod" ]]; then
    # production version
    ./package-module.sh "${2:-$(date +'%Y%m%d')}" --prod
else
    # dev version
    ./package-module.sh  dev_${2:-$(date +'%Y%m%d')}
fi