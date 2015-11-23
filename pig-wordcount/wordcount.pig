text = LOAD 'alice/alice-in-wonderland.txt' AS (line:CHARARRAY);
tokens = FOREACH text GENERATE flatten(TOKENIZE(line)) AS word;
words = GROUP tokens BY word;
counts = FOREACH words GENERATE group,COUNT(tokens) AS cnt;
sorted = ORDER counts BY cnt DESC;
STORE sorted INTO 'alice_wordcount';
