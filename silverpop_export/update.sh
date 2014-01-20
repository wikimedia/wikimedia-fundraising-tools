#!/usr/bin/env sh
echo "Start" >> log.log
date >> log.log

echo "Regenerating db table"
mysql mwalker < update_table.sql > /dev/null

echo "Exporting whole table"
mysql mwalker < export_all.sql > DatabaseUpdate.tsv

echo "Exporting unsubscribes"
mysql mwalker < export_unsubscribes.sql > Unsubscribes.tsv

echo "Archiving files"
DATE=$(date +"%Y%m%d_%H%M")
cp DatabaseUpdate.tsv old/full_$DATE.tsv
cp Unsubscribes.tsv old/unsub_$DATE.tsv

# These are long running, so maybe send email when it's done
# echo "Now go put files on the server" | mail -s "Silverpop export job finished" pcoombe@wikimedia.org

echo "End" >> log.log
date >> log.log