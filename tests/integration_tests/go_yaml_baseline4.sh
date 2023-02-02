# Note, eodatasets uses the actual zip, rather than the
# soft linked zip as the zip location in the yaml
# therefor I'm going to move some actual zips and assume they are there,
# along with the dir structure


S2L1DIR='/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/'
AUTODIR='/g/data/u46/users/dsg547/test_data/c3/s2_autogen'
ZIPBASEDIR=zip
ZIPYAMLBASEDIR=zipyaml
YAMLBASEDIR=yaml
YEAR=2022
YEARMONTH=2022-02
DEG5=15S140E-20S145E
ZIP2=S2B_MSIL1C_20220228T004659_N0400_R102_T54KWF_20220228T015542.zip

MIDDIRS=$YEAR/$YEARMONTH/$DEG5/
ZIPDIR=$AUTODIR/$ZIPBASEDIR/$DEG5
YAMLDIR=$AUTODIR/$YAMLBASEDIR/$DEG5
ZIPFILE=$S2L1DIR/$MIDDIRS/$ZIP2

mkdir -p $ZIPDIR
mkdir -p $YAMLDIR

# This is quick actually
cp $ZIPFILE $ZIPDIR/
#exit 1

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles
module load eodatasets3

# YAMLDIR
eo3-prepare sentinel-l1 $ZIPDIR/$ZIP2 --output-base $YAMLDIR
