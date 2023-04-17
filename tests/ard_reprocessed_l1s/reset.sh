#!/usr/bin/env bash

rm -rf ../test_data/ls9_reprocessing/ga_ls9c_ard_3/
cp -r ../test_data/ls9_reprocessing/a_ga_ls9c_ard_3_raw/ ../test_data/ls9_reprocessing/ga_ls9c_ard_3/

# comment out if not archiving
./db_index.sh