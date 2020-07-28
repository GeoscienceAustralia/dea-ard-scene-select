#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

# Other DEA check-code.sh's do not have this....
module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles
module load dea/20190329
module load wagl/5.4.1

set -eu
set -x

python3 -m pytest tests
python3 -m pycodestyle scene_select --max-line-length 120
python3 -m pylint -j 2 -d line-too-long --reports no scene_select

# Run this to auto format the python code
# black -l 120  tests/ scene_select/
