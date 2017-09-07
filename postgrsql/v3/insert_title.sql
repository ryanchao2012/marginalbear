CREATE TEMP TABLE tmp_title AS SELECT * FROM pttcorpus_title WITH NO DATA;
\copy tmp_title(ctype,tokenizer,tokenized,grammar,retrieval_count,post_id,quality) FROM '/var/local/marginalbear/data/tmp/title_formatted.csv'WITH CSV HEADER DELIMITER E'\t' ESCAPE '\' QUOTE '"';

INSERT INTO pttcorpus_title(ctype,category,tokenizer,tokenized,grammar,retrieval_count,quality,post_id) SELECT ctype,category,tokenizer,tokenized,grammar,retrieval_count,quality,post_id FROM tmp_title ON CONFLICT(post_id, tokenizer) DO NOTHING;

\copy (SELECT row_to_json(pdata) FROM (SELECT id, tokenized, grammar, tokenizer FROM pttcorpus_title WHERE (post_id, tokenizer) IN (SELECT post_id, tokenizer FROM tmp_title) ) pdata) TO '/var/local/marginalbear/data/title_dump2.jl';

DROP TABLE tmp_title;
