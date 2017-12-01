#!/bin/bash

basedir=/var/local/marginalbear/data
comment_dump=comment_dump.jl
vocab2comment=11_vocab2comment_formatted.csv

echo -e 'vocabulary_id\tcomment_id' > $basedir/$vocab2comment
echo "Format vocab2comment fields from title and vocabt dump."
pv $basedir/$comment_dump | parallel -j12 --pipe --round-robin --block 10K --line-buffer python format_vocab2comment.py >> $basedir/$vocab2comment
psql -U okbotadmin -d okbotdb -f insert_vocab2comment.sql


