CREATE TEMP TABLE tmp_cmt AS SELECT * FROM pttcorpus_comment WITH NO DATA;
\copy pg_temp_2.tmp_cmt(audience,floor,ctype,tokenizer,tokenized,grammar,retrieval_count,post_id,quality) FROM '/var/local/marginalbear/data/tmp/comment_formatted.csv'WITH CSV HEADER DELIMITER E'\t' ESCAPE '\' QUOTE '"';

INSERT INTO pttcorpus_comment(audience,floor,ctype,category,tokenizer,tokenized,grammar,retrieval_count,quality,post_id) SELECT audience,floor,ctype,category,tokenizer,tokenized,grammar,retrieval_count,quality,post_id FROM pg_temp_2.tmp_cmt ON CONFLICT(post_id, floor, tokenizer) DO NOTHING;

\copy (SELECT row_to_json(pdata) FROM (SELECT id, tokenized, grammar, tokenizer FROM pttcorpus_comment WHERE (post_id, floor, tokenizer) IN (SELECT post_id, floor, tokenizer FROM pg_temp_2.tmp_cmt) ) pdata) TO '/var/local/marginalbear/data/comment_dump2.jl';

DROP TABLE pg_temp_2.tmp_cmt;
