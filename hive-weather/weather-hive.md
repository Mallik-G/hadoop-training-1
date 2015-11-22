## Create Database

```sql
CREATE DATABASE training;
USE training;
```

## Create Tables
```sql
CREATE TABLE weather_2011(data STRING) STORED AS TEXTFILE;
```

```sql
LOAD DATA LOCAL INPATH '*.gz' INTO TABLE weather_2011;
```

```sql
SELECT * FROM weather_2011 limit 10;
```

```sql
SELECT SUBSTR(data,5,6) AS usaf FROM weather_2011 LIMIT 10;
```

```sql
SELECT 
    SUBSTR(data,5,6) AS usaf,
    SUBSTR(data,11,5) as wban, 
    SUBSTR(data,16,8) as date, 
    SUBSTR(data,24,4) as time,
    SUBSTR(data,42,5) as report_type,
    SUBSTR(data,61,3) as wind_direction, 
    SUBSTR(data,64,1) as wind_direction_qual, 
    SUBSTR(data,65,1) as wind_observation, 
    SUBSTR(data,66,4) as wind_speed,
    SUBSTR(data,70,1) as wind_speed_qual,
    SUBSTR(data,88,5) as air_temperature, 
    SUBSTR(data,93,1) as air_temperature_qual 
FROM weather_2011 
LIMIT 10;
```

```sql
DROP TABLE weather_2011;
```


## Using Partitions and Views

First we recreate the table with

```sql
CREATE TABLE weather_raw(data STRING) PARTITIONED BY(year STRING) STORED AS TEXTFILE;
```

We load local data into the table using the correct partition

```sql
LOAD DATA LOCAL INPATH '*.gz' INTO TABLE weather_raw PARTITION(year=2011);
```

Then we'll create a new view

```sql
CREATE VIEW weather AS
    SELECT 
        year,
        SUBSTR(`data`,5,6) AS `usaf`,
        SUBSTR(`data`,11,5) as `wban`, 
        SUBSTR(`data`,16,8) as `date`, 
        SUBSTR(`data`,24,4) as `time`,
        SUBSTR(`data`,42,5) as report_type,
        SUBSTR(`data`,61,3) as wind_direction, 
        SUBSTR(`data`,64,1) as wind_direction_qual, 
        SUBSTR(`data`,65,1) as wind_observation, 
        CAST(SUBSTR(`data`,66,4) AS FLOAT)/10 as wind_speed,
        SUBSTR(`data`,70,1) as wind_speed_qual,
        CAST(SUBSTR(`data`,88,5) AS FLOAT)/10 as air_temperature, 
        SUBSTR(`data`,93,1) as air_temperature_qual 
    FROM weather_raw; 
```

Et voila:
```sql
    select * from weather limit 10;
```

## Import ish Table

We also want to import the ish table, so we can lookup country names.

```sql
CREATE TABLE ish_raw(
    usaf STRING,
    wban STRING,
    name STRING,
    country STRING,
    fips STRING,
    state STRING,
    call STRING,
    latitude INT,
    longitude INT,
    elevation INT,
    date_begin STRING,
    date_end STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../ish-history.csv' OVERWRITE INTO TABLE ish_raw;
```

Now we can perform exactly the same query as in Java examples:
```sql
SELECT 
    ish.country,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather w
INNER JOIN ish_raw ish 
    ON w.usaf=ish.usaf 
    AND w.wban=ish.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY ish.country;
```

### For Impala

Since Impala cannot use the SERDE from above, we need to convert the table
in Hive. We use Parquet as file format.

```sql
CREATE TABLE ish 
    STORED AS PARQUET 
AS 
    SELECT * FROM ish_raw WHERE usaf <> 'USAF';
```
