CREATE TEMP TABLE tmp_audience AS SELECT * FROM pttcorpus_netizen WITH NO DATA;
\copy pg_temp_2.tmp_audience(name) FROM '/var/local/marginalbear/data/tmp/audience';
INSERT INTO pttcorpus_netizen(name) SELECT DISTINCT(name) FROM pg_temp_2.tmp_audience ON CONFLICT(name) DO NOTHING;
DROP TABLE pg_temp_2.tmp_audience;

