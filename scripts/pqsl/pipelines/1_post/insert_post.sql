CREATE TEMP TABLE tmp_post AS SELECT * FROM pttcorpus_post WITH NO DATA;

\copy tmp_post(id, tag, spider, url, author_id, publish_date, last_update, allow_update, quality) FROM '/var/local/marginalbear/data/1_post_formatted.csv' CSV HEADER DELIMITER E'\t';

INSERT INTO pttcorpus_post SELECT * FROM tmp_post ON CONFLICT(url) DO NOTHING;

DROP TABLE tmp_post;

