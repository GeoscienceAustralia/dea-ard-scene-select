#!/usr/bin/env bash
# Convenience script for running checks.
./run_pylint.sh
./run_black.sh
tests/do_tests.sh
