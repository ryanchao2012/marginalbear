#!/bin/bash

basedir=data/tmp


echo "Copying title_dump.jl"
cp -f data/title_dump.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/title_dump.jl
echo -e 'ctype\ttokenizer\ttokenized\tgrammar\tretrieval_count\tpost\tquality' > $basedir/title_formatted.csv
echo "Format post fields from crawler dump."
pv $basedir/title_dump.jl | parallel -j7 --pipe --round-robin --block 10K --line-buffer python format_title.py >> $basedir/title_formatted.csv
rm -f $basedir/title_dump.jl
psql -U okbotadmin -d okbotdb -f insert_title.sql
echo "Copying title_dump2.jl"
cp -f data/title_dump2.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/title_dump2.jl








# rm -rf $basedir


