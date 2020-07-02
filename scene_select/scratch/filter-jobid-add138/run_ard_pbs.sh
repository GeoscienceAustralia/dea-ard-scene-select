#!/bin/bash
module unload dea
source None

ard_pbs --level1-list /home/osboxes/sandbox/dea-ard-scene-select/ard_scene_select/scratch/filter-jobid-add138/scenes_to_ARD_process.txt --pkgdir /home/osboxes/sandbox/dea-ard-scene-select/ard_scene_select/scratch --logdir /home/osboxes/sandbox/dea-ard-scene-select/ard_scene_select/scratch --project v10 --walltime 05:00:00 --nodes 1 --workdir /home/osboxes/sandbox/dea-ard-scene-select/ard_scene_select/scratch
