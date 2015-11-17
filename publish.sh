#!/bin/sh
echo "Running publisher $5 times"
for i in `seq 1 $5`
  do
    nohup  java -jar target/debs-data-publisher-randomized-1.0.0-jar-with-dependencies.jar $1 $2 $3 $4 > /dev/null 2>&1 &
 done