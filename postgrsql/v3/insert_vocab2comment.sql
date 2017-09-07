CREATE TEMP TABLE tmp_v2c AS SELECT * FROM pttcorpus_vocabulary_comment WITH NO DATA;
\copy tmp_v2c(vocabulary_id, comment_id) FROM '/var/local/marginalbear/data/tmp/vocab2comment_formatted.csv' CSV HEADER DELIMITER E'\t'; 

INSERT INTO pttcorpus_vocabulary_comment(vocabulary_id, comment_id) SELECT vocabulary_id, comment_id FROM tmp_v2c ON CONFLICT(vocabulary_id, comment_id) DO NOTHING;

DROP TABLE tmp_v2c;
