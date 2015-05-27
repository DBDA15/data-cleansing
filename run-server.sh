rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit --class de.hpi.fgis.willidennis.Main --master spark://172.16.21.111:7077 scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 10 2 > perf_log.txt 2>&1
wc -l result/*

rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit --class de.hpi.fgis.willidennis.Main --master spark://172.16.21.111:7077 scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 100 2 > perf_log.txt 2>&1
wc -l result/*
