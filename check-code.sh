#!/usr/bin/env bash

if [[ $HOSTNAME == *"LAPTOP-UOJEO8EI"* ]]; then
  echo "duncans laptop"
  echo "conda activate dea2023"
  echo "sudo service postgresql start"
fi

# Convenience script for running checks.
./run_pylint.sh

if [[ $HOSTNAME == *"gadi"* ]]; then
    ./run_black.sh
fi
tests/do_tests.sh

# WARNING
# Needs postgres running
tests/integration_tests/launch_test.sh
