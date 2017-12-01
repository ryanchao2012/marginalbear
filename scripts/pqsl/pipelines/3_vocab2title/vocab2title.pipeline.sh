#!/bin/bash

basedir=/var/local/marginalbear/data
title_dump=title_dump.jl
vocab2title=6_vocab2title_formatted.csv

echo -e 'vocabulary_id\ttitle_id' > $basedir/$vocab2title
echo "Format vocab2title fields from title and vocabt dump."
pv $basedir/$title_dump | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_vocab2title.py >> $basedir/$vocab2title

psql -U okbotadmin -d okbotdb -f insert_vocab2title.sql


