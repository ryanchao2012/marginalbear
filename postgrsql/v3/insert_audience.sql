CREATE TEMP TABLE tmp_audience AS SELECT * FROM pttcorpus_netizen WITH NO DATA;
\copy tmp_audience(name) FROM '/var/local/marginalbear/data/tmp/audience' WITH CSV DELIMITER E'\t' ESCAPE '\' QUOTE '"';
INSERT INTO pttcorpus_netizen(name) SELECT DISTINCT(name) FROM tmp_audience ON CONFLICT(name) DO NOTHING;
DROP TABLE tmp_audience;

