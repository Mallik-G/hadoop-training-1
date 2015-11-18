hadoop  jar $HADOOP_HOME/hadoop-streaming.jar \
    -input myInputDirs \
    -output myOutputDir \
    -mapper mapper.py \
    -reducer reducer.py

cat mapper.py | ./mapper.py | sort | ./reducer.py 

