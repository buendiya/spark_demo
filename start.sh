#!/usr/bin/env bash

class_name="$1"
# spark2-submit --master yarn --class  $class_name  demo-1.0-SNAPSHOT.jar
spark2-submit \
    --master local[4] \
    --conf "spark.dynamicAllocation.enabled=false" \
    --num-executors 10 --executor-cores 1 --conf spark.yarn.executor.memoryOverhead=2048 \
    --class  $class_name  demo-1.0-SNAPSHOT.jar

