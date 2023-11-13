
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  host=deadev.nci.org.au
else
  echo "not NCI"
   host=localhost
fi

psql -h $host $USER -d ${USER}_dev -a -f db_delete_odc.sql
