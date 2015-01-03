#!/bin/bash
cd /home/bingweiliu/NBClassifier
total_t=0
for i in {1..10}
do
 # prepare data
 hdfs dfs -mv review-in/POS-$i.csv test/
 hdfs dfs -mv review-in/NEG-$i.csv test/
 j=$(($i-1))
 if [ $j -gt 0 ]; then
  hdfs dfs -mv test/POS-$j.csv review-in/
  hdfs dfs -mv test/NEG-$j.csv review-in/
 fi
a=`date +%s%N`
hadoop jar NBClassifier.jar naivebayes.NBController -D train=review-in -D test=test -D output=reviewout -D reducer=20 -D result=result$i 
b=`date +%s%N`
t=`echo "scale=6; ($b-$a)/10^9" | bc`
total_t=`echo "$total_t+$t" | bc`
echo "$1,$i,$t" >> ~/NBClassifier/allresult/detailtime.csv
#retrieve model
cd ~/NBClassifier/allresult/$1/trial$i
rm -f part-r*
hdfs dfs -copyToLocal model/part-r* .
cat part-r* > model.csv
# retrieve output
rm -f part-r*
hdfs dfs -copyToLocal testprepare/part-r* .
cat part-r* > testprepare.csv
#
rm -f part-r*
hdfs dfs -copyToLocal reviewout/part-r* .
cat part-r* > reviewout.csv
rm -f part-r*

cd ~/NBClassifier
done
echo "$1,$total_t" >> ~/NBClassifier/allresult/total_time.csv
