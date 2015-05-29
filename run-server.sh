rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup /opt/spark/spark-1.3.0/bin/spark-submit --class de.hpi.fgis.willidennis.Main --master spark://172.16.21.111:7077 scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 10 2 hdfs://tenemhead2/data/data-cleansing/out/result10 > perf_log.txt 2>&1
wc -l result/*

#rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
#nohup /opt/spark/spark-1.3.0/bin/spark-submit --class de.hpi.fgis.willidennis.Main --master spark://172.16.21.111:7077 scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 100 2 hdfs://tenemhead2/data/data-cleansing/out/result > perf_log.txt 2>&1
#wc -l result/*
