# WordCount
CREATE TABLE alice(row STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../alice/*.txt" OVERWRITE INTO TABLE alice;

SELECT 
    TRIM(EXPLODE(SPLIT(row,' '))) AS word 
FROM alice
LIMIT 10; 

SELECT 
    w.word AS word,
    SUM(1) AS cnt 
FROM (
    SELECT 
        EXPLODE(SPLIT(row,' ')) AS word 
    FROM alice) as w 
WHERE
    TRIM(w.word) <> ''
GROUP BY w.word
ORDER BY cnt DESC 
LIMIT 3;

