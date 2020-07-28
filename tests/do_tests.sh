#!/bin/bash

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles
module load dea/20190329
module load wagl/5.4.1

SSPATH=$PWD/../

# so it uses the dev scene select
#echo $PYTHONPATH
[[ ":$PYTHONPATH:" != *":$SSPATH:"* ]] && PYTHONPATH="$SSPATH:${PYTHONPATH}"
#echo $PYTHONPATH

pytest -s test_check_ancillary.py #-k 'test_definitive_ancillary_filesII'
