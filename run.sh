#!/bin/bash
for i in `seq 100 100 1000`
do
j=$(($i*1000))
echo "--Start $j"
cd /home/bingweiliu/NBClassifier
dataset/maketrial $j
hdfs dfs -rm -skipTrash review-in/*
hdfs dfs -rm -skipTrash test/*
hdfs dfs -copyFromLocal dataset/pos/POS* review-in/
hdfs dfs -copyFromLocal dataset/neg/NEG* review-in/

./runhadoop.sh $j 

cd ~/NBClassifier/allresult/$j/
rm -f part-r*
rm -f result*
hdfs dfs -copyToLocal result/* .
cat result*.txt > result.csv
echo "--End $j"
done
