CREATE TEMP TABLE tmp_title AS SELECT * FROM pttcorpus_title WITH NO DATA;
\copy pg_temp_2.tmp_title(ctype,tokenizer,tokenized,grammar,retrieval_count,post,quality) 
FROM '/var/local/marginalbear/data/tmp/title_formatted.csv' CSV HEADER DELIMITER E'\t';
\copy (SELECT row_to_json(pdata) 
    FROM (SELECT id, tokenized, grammar, tokenizer FROM pttcorpus_title) pdata)
    WHERE id IN
        (INSERT INTO pttcorpus_title 
        SELECT * FROM pg_temp_2.tmp_title 
        ON CONFLICT(post_id, tokenizer) DO 
        UPDATE SET
            retrieval_count = pttcorpus_title.retrieval_count
        RETURNING id)
    TO '/var/local/marginalbear/data/title_dump2.jl';

DROP TABLE pg_temp_2.tmp_title;