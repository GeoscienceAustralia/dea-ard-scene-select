#!/usr/bin/env bash
# Convenience script for running black
module use /g/data/v10/public/modules/modulefiles
module load dea

black -l 120  tests/ scene_select/
