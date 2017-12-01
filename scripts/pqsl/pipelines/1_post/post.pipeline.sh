#!/bin/bash

basedir=/var/local/marginalbear/data
input=post_dump.jl
target=1_post_formatted.csv
header='id\ttag\tspider\turl\tauthor_id\tpublish_date\tlast_update\tallow_update\tquality'

echo "Copy posts from postgresql"
echo -e $header > $basedir/$target

echo "Format post fields"
# psql -U okbotadmin -d okbotdb  -c "\copy (SELECT row_to_json(data) FROM (SELECT * FROM pttcrawler_post) as data) TO STDOUT" | sed -e 's/\\\\/\\/g' > post_dump.jl 
pv $basedir/$input | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_post.py >> $basedir/$target

echo "Extract authors from ${target}."
sed 1d $basedir/$target | cut -f 5 | sort --parallel=8 | uniq > $basedir/2_author

echo "Insert authors to table pttcorpus_netizen."
psql -U okbotadmin -d okbotdb -f insert_author.sql

echo "Insert posts to table pttcorpus_post."
psql -U okbotadmin -d okbotdb -f insert_post.sql

