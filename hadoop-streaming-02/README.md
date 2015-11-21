hadoop  jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper.py,reducer.py,ish-history.csv \
    -input alice \
    -output alice-wc \
    -mapper mapper.py \
    -reducer reducer.py

zcat /data/weather/*.gz | ./mapper.py | sort | ./reducer.py 
