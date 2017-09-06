#!/bin/bash

basedir=data/tmp

echo "Copying vocabc.jl"
cp -f data/vocabc.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/vocabc.jl

echo "Copying comment_dump2.jl"
cp -f data/comment_dump2.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/comment_dump2.jl
echo -e 'vocabulary_id\tcomment_id' > $basedir/vocab2comment_formatted.csv
echo "Format vocab2comment fields from title and vocabt dump."
pv $basedir/comment_dump2.jl | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_vocab2comment.py | sed /^$/d >> $basedir/vocab2comment_formatted.csv
psql -U okbotadmin -d okbotdb -f insert_vocab2comment.sql


