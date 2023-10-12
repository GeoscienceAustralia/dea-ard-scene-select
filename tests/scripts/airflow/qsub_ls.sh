WORK_DIR='/home/547/dsg547/dump/airflow/qsub_ls_results'

module use /g/data/v10/public/modules/modulefiles;
# module load dea; # not doing this so the output from this call is clean
# if all goes well the output looks like
# 10035142.gadi-pbs

mkdir -p $WORK_DIR
cd $WORK_DIR

qsub -N qsub_ls \
              -q express \
              -W umask=33 \
              -l wd,walltime=0:01:00,mem=1GB,ncpus=1 -m abe \
              -l storage=gdata/v10+gdata/u46+gdata/fk4+gdata/rs0+gdata/if87 \
              -P u46 -o $WORK_DIR -e $WORK_DIR \
              -- /bin/bash -l -c \
                  "module use /g/data/v10/public/modules/modulefiles/; \
                  module use /g/data/v10/private/modules/modulefiles/; \
                  module load ard-scene-select-py3-dea/20231010; \
                  ard-scene-select --help"
