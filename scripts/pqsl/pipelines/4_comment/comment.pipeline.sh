#!/bin/bash

basedir=/var/local/marginalbear/data
post_dump=post_dump.jl
comment_dump=comment_dump.jl
comment_header='audience\tfloor\tctype\ttokenizer\ttokenized\tgrammar\tretrieval_count\tpost\tquality'
comment=7_comment_formatted.csv
audience=8_audience

vocabc_header='word\tpos\ttokenizer\ttitlefreq\tcontentfreq\tcommentfreq\tstopword\tquality'
vocabc=9_vocabc.csv
vocabc_dump=10_vocabc.jl


# echo -e $comment_header > $basedir/$comment
# echo "Format comment fields from crawler dump."
# pv $basedir/$post_dump | parallel -j8 --pipe --round-robin --block 10K --line-buffer python format_comment.py >> $basedir/$comment
# 
# echo "Extract audience from comment_formatted.csv."
# sed 1d $basedir/$comment | cut -f 1 | sort --parallel=8 | uniq > $basedir/$audience

# echo "Insert audiences to table pttcorpus_netizen."
# psql -U okbotadmin -d okbotdb -f insert_audience.sql

# echo "Insert comments to table pttcorpus_comment."
# psql -U okbotadmin -d okbotdb -f insert_comment.sql

# echo "Dump comment to get id."
# psql -U okbotadmin -d okbotdb  -c "\copy (SELECT row_to_json(data) FROM (SELECT id, tokenized, grammar, tokenizer FROM pttcorpus_comment) as data) TO STDOUT" | sed -e 's/\\\\/\\/g' > $basedir/$comment_dump
### -- \copy (SELECT row_to_json(pdata) FROM (SELECT id, tokenized, grammar, tokenizer FROM pttcorpus_comment WHERE (post_id, floor, tokenizer) IN (SELECT post_id, floor, tokenizer FROM tmp_cmt) ) pdata) TO '/var/local/marginalbear/data/comment_dump.jl';

# echo -e $vocabc_header > $basedir/$vocabc
# pv $basedir/$comment_dump | parallel -j12 --pipe --round-robin --block 10K python ../extract_vocab.py | sort --parallel=8 | uniq >> $basedir/$vocabc
 
echo "Insert vocabc to table pttcorpus_vocabulary."
psql -U okbotadmin -d okbotdb -f insert_vocabc.sql

psql -U okbotadmin -d okbotdb  -c "\copy (SELECT row_to_json(pdata) FROM (SELECT id, word, pos, tokenizer FROM pttcorpus_vocabulary) pdata) TO STDOUT" | sed -e 's/\\\\/\\/g' > $basedir/$vocabc_dump

