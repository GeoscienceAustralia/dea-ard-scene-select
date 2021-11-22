#!/usr/bin/env bash
# This doesn't work in vdi
#module load agdc-py3/1.1.8

psql -h deadev.nci.org.au dsg547 -d dsg547_dev -a -f db_delete_odc.sql
