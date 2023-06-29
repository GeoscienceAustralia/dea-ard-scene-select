#!/usr/bin/env bash

if [[ $HOSTNAME == *"gadi"* ]]; then
    module use /g/data/v10/public/modules/modulefiles
    module use /g/data/v10/private/modules/modulefiles

    module load ard-pipeline/20211222 # for pylint
fi

python3 -m pylint -j 2 -d line-too-long --reports no scene_select tests scripts
