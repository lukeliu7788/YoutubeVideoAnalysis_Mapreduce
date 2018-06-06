#!/bin/env bash

if [ $# -ne 4 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./cou_driver.sh [input_location] [output_location] [country1] [country2]"
    exit 1
fi

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-D mapreduce.job.reduces=3 \
-file cou_map.py \
-mapper 'python3 cou_map.py' \
-file cou_reduse.py \
-reducer 'python3 cou_reduse.py' \
-input $1 \
-output $2 \
-cmdenv "country1"=$3 \
-cmdenv "country2"=$4 
