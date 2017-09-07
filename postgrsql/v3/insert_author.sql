CREATE TEMP TABLE tmp_author AS SELECT * FROM pttcorpus_netizen WITH NO DATA;
\copy tmp_author(name) FROM '/var/local/marginalbear/data/tmp/author';
INSERT INTO pttcorpus_netizen(name) SELECT DISTINCT(name) FROM tmp_author ON CONFLICT(name) DO NOTHING;
DROP TABLE tmp_author;

