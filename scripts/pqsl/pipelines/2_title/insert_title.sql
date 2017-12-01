CREATE TEMP TABLE tmp_title AS SELECT * FROM pttcorpus_title WITH NO DATA;
\copy tmp_title(ctype, tokenizer, tokenized, grammar, retrieval_count, post_id, quality) FROM '/var/local/marginalbear/data/3_title_formatted.csv' WITH CSV HEADER DELIMITER E'\t' ESCAPE '\' QUOTE '"';

INSERT INTO pttcorpus_title(ctype, category, tokenizer, tokenized, grammar, retrieval_count, quality, post_id) SELECT ctype, category, tokenizer, tokenized, grammar, retrieval_count, quality, post_id FROM tmp_title ON CONFLICT(post_id, tokenizer) DO NOTHING;

DROP TABLE tmp_title;
