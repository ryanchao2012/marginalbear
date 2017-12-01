CREATE TEMP TABLE tmp_cmt AS SELECT * FROM pttcorpus_comment WITH NO DATA;
\copy tmp_cmt(audience_id, floor, ctype, tokenizer, tokenized, grammar, retrieval_count, post_id, quality) FROM '/var/local/marginalbear/data/7_comment_formatted.csv'WITH CSV HEADER DELIMITER E'\t' ESCAPE '\' QUOTE '"';

INSERT INTO pttcorpus_comment(audience_id, floor, ctype, tokenizer, tokenized, grammar, retrieval_count, quality, post_id) SELECT audience_id, floor, ctype, tokenizer, tokenized, grammar, retrieval_count, quality, post_id FROM tmp_cmt ON CONFLICT(post_id, floor, tokenizer) DO NOTHING;

DROP TABLE tmp_cmt;
