#!/bin/bash

basedir=/var/local/marginalbear/data
post_dump=post_dump.jl
title_dump=title_dump.jl
# post=1_post_formatted.csv
title=3_title_formatted.csv
vocabt=4_vocabt.csv
vocabt_dump=5_vocabt.jl

title_header='ctype\ttokenizer\ttokenized\tgrammar\tretrieval_count\tpost\tquality'
vocabt_header='word\tpos\ttokenizer\ttitlefreq\tcontentfreq\tcommentfreq\tstopword\tquality'

echo "Format post fields from crawler dump."
echo -e $title_header > $basedir/$title
pv $basedir/$post_dump | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_title.py >> $basedir/$title

echo "Insert title to table pttcorpus_title."
psql -U okbotadmin -d okbotdb -f insert_title.sql

echo "Dump title to get id."
psql -U okbotadmin -d okbotdb  -c "\copy (SELECT row_to_json(data) FROM (SELECT * FROM pttcorpus_title) as data) TO STDOUT" | sed -e 's/\\\\/\\/g' > $basedir/$title_dump

echo -e $vocabt_header > $basedir/$vocabt
pv $basedir/$title_dump | parallel -j8 --pipe --round-robin --block 10K python ../extract_vocab.py | sort --parallel=8 | uniq >> $basedir/$vocabt

echo "Insert vocabt to table pttcorpus_vocabulary."
psql -U okbotadmin -d okbotdb -f insert_vocabt.sql
psql -U okbotadmin -d okbotdb  -c "\copy (SELECT row_to_json(pdata) FROM (SELECT id, word, pos, tokenizer FROM pttcorpus_vocabulary) pdata) TO STDOUT" | sed -e 's/\\\\/\\/g' > $basedir/$vocabt_dump;

