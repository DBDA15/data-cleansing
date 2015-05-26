rm -rf result/
# fetches data from remote server. change for your local training_set dir at your will
spark-submit scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --class scala/target/classes/de/hpi/fgis/willidennis/Main hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/ 10 2
wc -l result/*