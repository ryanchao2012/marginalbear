#!/bin/bash

basedir=data/tmp


echo "Copying comment_dump.jl"
cp -f data/comment_dump.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/comment_dump.jl
echo -e 'audience\tfloor\tctype\ttokenizer\ttokenized\tgrammar\tretrieval_count\tpost\tquality' > $basedir/comment_formatted.csv
echo "Format comment fields from crawler dump."
pv $basedir/comment_dump.jl | parallel -j7 --pipe --round-robin --block 10K --line-buffer python format_comment.py >> $basedir/comment_formatted.csv


echo "Extract audience from comment_formatted.csv."
sed 1d $basedir/comment_formatted.csv | cut -f 1 | sort | uniq | sed /^$/d > $basedir/audience
echo "Insert audiences to table pttcorpus_netizen."
psql -U okbotadmin -d okbotdb -f insert_audience.sql
echo "Insert comments to table pttcorpus_comment."
psql -U okbotadmin -d okbotdb -f insert_comment.sql

echo "Copying comment_dump2.jl"
cp -f data/comment_dump2.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/comment_dump2.jl
echo -e 'word\tpos\ttokenizer\ttitlefreq\tcontentfreq\tcommentfreq\tstopword\tquality' > $basedir/vocabc.csv
pv $basedir/comment_dump2.jl | parallel -j6 --pipe --round-robin --block 10K python extract_vocab.py | sort | uniq | sed /^$/d >> $basedir/vocabc.csv

echo "Insert vocabc to table pttcorpus_vocabulary."
psql -U okbotadmin -d okbotdb -f insert_vocabc.sql

