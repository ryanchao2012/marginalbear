CREATE TEMP TABLE tmp_vocabt AS SELECT * FROM pttcorpus_vocabulary WITH NO DATA;
\copy tmp_vocabt(word, pos, tokenizer, titlefreq, contentfreq, commentfreq, stopword, quality) FROM '/var/local/marginalbear/data/tmp/vocabt.csv' CSV HEADER DELIMITER E'\t' QUOTE '"' ESCAPE '\'; 

INSERT INTO pttcorpus_vocabulary(word,pos,tokenizer,titlefreq, contentfreq, commentfreq,stopword,quality) SELECT word,pos,tokenizer,titlefreq, contentfreq, commentfreq,stopword,quality FROM tmp_vocabt ON CONFLICT(word, pos, tokenizer) DO NOTHING;

\copy (SELECT row_to_json(pdata) FROM (SELECT id, word, pos, tokenizer FROM pttcorpus_vocabulary WHERE (word, pos, tokenizer) IN (SELECT word, pos, tokenizer FROM tmp_vocabt) ) pdata) TO '/var/local/marginalbear/data/vocabt.jl';

DROP TABLE tmp_vocabt;
