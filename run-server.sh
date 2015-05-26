rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --class scala/target/classes/de/hpi/fgis/willidennis/Main hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 10 2 > perf_log.txt
wc -l result/*

rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --class scala/target/classes/de/hpi/fgis/willidennis/Main hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 100 2 > perf_log.txt
wc -l result/*

rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --class scala/target/classes/de/hpi/fgis/willidennis/Main hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 1000 2 > perf_log.txt
wc -l result/*

rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
nohup spark-submit scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --class scala/target/classes/de/hpi/fgis/willidennis/Main hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 10000 2 > perf_log.txt
wc -l result/*