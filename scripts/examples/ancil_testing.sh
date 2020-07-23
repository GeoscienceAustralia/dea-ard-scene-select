#!/bin/bash


module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles
module use /home/547/dsg547/devmodules/modulefiles

#module load ard-scene-select-py3-dea/20200717
module load ard-scene-select-py3-dea/20200724
echo $PYTHONPATH
pytest ../../tests
