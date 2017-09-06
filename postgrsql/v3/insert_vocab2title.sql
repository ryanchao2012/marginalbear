CREATE TEMP TABLE tmp_v2t AS SELECT * FROM pttcorpus_vocabulary_title WITH NO DATA;
\copy pg_temp_2.tmp_v2t(vocabulary_id, title_id) FROM '/var/local/marginalbear/data/tmp/vocab2title_formatted.csv' CSV HEADER DELIMITER E'\t'; 

INSERT INTO pttcorpus_vocabulary_title(vocabulary_id, title_id) SELECT vocabulary_id, title_id FROM pg_temp_2.tmp_v2t ON CONFLICT(vocabulary_id, title_id) DO NOTHING;

DROP TABLE pg_temp_2.tmp_v2t;
