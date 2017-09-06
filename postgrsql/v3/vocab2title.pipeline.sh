#!/bin/bash

basedir=data/tmp

echo "Copying vocabt.jl"
cp -f data/vocabt.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/vocabt.jl

echo "Copying title_dump2.jl"
cp -f data/title_dump2.jl $basedir/
sed -i -e 's/\\\\/\\/g' $basedir/title_dump2.jl
echo -e 'vocabulary_id\ttitle_id' > $basedir/vocab2title_formatted.csv
echo "Format vocab2title fields from title and vocabt dump."
pv $basedir/title_dump2.jl | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_vocab2title.py | sed /^$/d >> $basedir/vocab2title_formatted.csv
#rm -f $basedir/title_dump2.jl
psql -U okbotadmin -d okbotdb -f insert_vocab2title.sql


