#!/usr/bin/env bash
rm -rf scratch_ls/ga*
./delete_odc.sh
./db_index.sh
./ls_go_select.sh
./s2_go_select.sh
