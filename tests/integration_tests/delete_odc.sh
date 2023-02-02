
if [[ $HOSTNAME == *"gadi"* ]]; then
  echo "gadi - NCI"
  host=deadev.nci.org.au
else
  echo "not NCI"
   host=localhost
fi

psql -h $host $USER -d dsg547_dev -a -f db_delete_odc.sql
