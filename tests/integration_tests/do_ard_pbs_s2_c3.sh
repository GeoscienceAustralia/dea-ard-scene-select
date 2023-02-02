
tag=yamls_dir
#tag=zipyaml
PKGDIR=pkgdir_$tag
LOGDIR=logdir_$tag
WORKDIR=workdir_$tag
#tag=zip
L1_LIST=s2_$tag.txt

yamdir=''
yamdir=' --yamls-dir /g/data/u46/users/dsg547/test_data/s2/autogen/yaml/2022/2022-01/'


#WORKDIR=/g/data/up71/tmp/s2_dass

mkdir -p $WORKDIR
mkdir -p $LOGDIR
mkdir -p $PKGDIR

DEFENV=definitive.env

. $DEFENV

loc=$PWD
echo $loc
more $loc/$L1_LIST
ard_pbs --help
which ard_pbs
ard_pbs --level1-list $loc/$L1_LIST --workdir  $loc/$WORKDIR --logdir $loc/$LOGDIR --pkgdir $loc/$PKGDIR --env $loc/$DEFENV --workers 1 --nodes 1 --memory 192 --jobfs 50 --project u46 --queue express --walltime 03:00:00 $yamdir --email duncan.gray@ga.gov.au

# Note the yaml has also been moved to;
# /g/data/up71/projects/s2_l1c_yamls/20S140E-25S145E

# --level1-list $loc/s2_l1_up71.txt

