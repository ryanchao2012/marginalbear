CREATE TEMP TABLE tmp_author AS SELECT * FROM pttcorpus_netizen WITH NO DATA;
\copy pg_temp_2.tmp_author(name) FROM '/var/local/marginalbear/data/tmp/author';
INSERT INTO pttcorpus_netizen(name) SELECT DISTINCT(name) FROM pg_temp_2.tmp_author ON CONFLICT(name) DO NOTHING;
DROP TABLE pg_temp_2.tmp_author;

