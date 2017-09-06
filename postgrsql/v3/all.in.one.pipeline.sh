#!/bin/bash

basedir=data/tmp
# mkdir $basedir

echo "Copying post_dump.jl"
cp -f data/post_dump.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/post_dump.jl
echo -e 'id\ttag\tspider\turl\tauthor_id\tpublish_date\tlast_update\tallow_update\tquality' > $basedir/post_formatted.csv
echo "Format post fields from crawler dump."
pv $basedir/post_dump.jl | parallel -j4 --pipe --round-robin --block 10K --line-buffer python format_post.py >> $basedir/post_formatted.csv
rm -f $basedir/post_dump.jl
echo "Extract authors from post_formatted.csv."
sed 1d $basedir/post_formatted.csv | cut -f 5 | sort | uniq | sed /^$/d > $basedir/author
echo "Insert authors to table pttcorpus_netizen."
psql -U okbotadmin -d okbotdb -f insert_author.sql
psql -U okbotadmin -d okbotdb -f insert_post.sql


# rm -rf $basedir


