cd flink
sudo -u hadoop10 /home/hadoop10/bin/start-flink-cluster.sh flink-0.9.0 && \
batchStats.sh &> 2015-07-13-scaleout-repetitions-flink.log
sudo -u hadoop10 /home/hadoop10/bin/stop-flink-cluster.sh flink-0.9.0 && \
cd ..
#####
cd spark
sudo -u hadoop10 /home/hadoop10/bin/start-spark-cluster.sh && \
batchStats-spark.sh &> 2015-07-13-scaleout-repetitions-spark.log
sudo -u hadoop10 /home/hadoop10/bin/stop-spark-cluster.sh