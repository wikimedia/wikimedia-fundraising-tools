#!/bin/bash

LPATH=$(dirname $(readlink -f $0))
EMAIL=${1:-$(whoami)"@wikimedia.org"}
if [ -f ./hashpass ]
  then
    HASHPASS=$(head -n 1 ./hashpass)
  else
    echo "hashpass file not found!"
    exit 1
fi

echo "Start" >> log.log
date >> log.log

echo "Regenerating db table"
mysql mwalker < $LPATH/update_table.sql > /dev/null

echo "Writing the unsubscribe hash information"
mysql -e "UPDATE silverpop_export ex SET unsub_hash = SHA1(CONCAT(last_ctid, email, $HASHPASS ));" mwalker > /dev/null

echo "Exporting whole table"
mysql mwalker < $LPATH/export_all.sql > DatabaseUpdate.tsv

echo "Exporting unsubscribes"
mysql mwalker < $LPATH/export_unsubscribes.sql > Unsubscribes.tsv

echo "Archiving files"
DATE=$(date +"%Y%m%d_%H%M")
cp DatabaseUpdate.tsv old/full_$DATE.tsv
cp Unsubscribes.tsv old/unsub_$DATE.tsv

echo "Sending email on finish"
echo "Now go put files on the server" | mail -s "Silverpop export job finished" $EMAIL

echo "End" >> log.log
date >> log.log
