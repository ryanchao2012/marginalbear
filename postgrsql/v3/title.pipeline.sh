#!/bin/bash

basedir=data/tmp


echo "Copying title_dump.jl"
cp -f data/title_dump.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/title_dump.jl


echo "Format post fields from crawler dump."
echo -e 'ctype\ttokenizer\ttokenized\tgrammar\tretrieval_count\tpost\tquality' > $basedir/title_formatted.csv
pv $basedir/title_dump.jl | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_title.py >> $basedir/title_formatted.csv

psql -U okbotadmin -d okbotdb -f insert_title.sql

echo "Copying title_dump2.jl"
cp -f data/title_dump2.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/title_dump2.jl

echo -e 'word\tpos\ttokenizer\ttitlefreq\tcontentfreq\tcommentfreq\tstopword\tquality' > $basedir/vocabt.csv
pv $basedir/title_dump2.jl | parallel -j6 --pipe --round-robin --block 10K python extract_vocab.py | sort | uniq | sed /^$/d >> $basedir/vocabt.csv

echo "Insert vocabt to table pttcorpus_vocabulary."
psql -U okbotadmin -d okbotdb -f insert_vocabt.sql






