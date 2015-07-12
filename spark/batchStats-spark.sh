#!/usr/bin/env bash
SPARK="/opt/spark/spark-1.3.0/bin/spark-submit"
# SPARK=spark-submit
JAR="target/findSimilarNetflix-1.0.jar"
INPUT="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
# INPUT="../netflixdata/cat"
LOG_DIR="out/mylogs/spark"
# LOG_DIR="."
OUTPUT_DIR="hdfs://tenemhead2/data/data-cleansing/spark/out"
# OUTPUT_DIR="/tmp/spark-output"
BUILD="1.0"
# MASTER="spark://172.16.21.111:7077"
# MASTER="localhost:4041"

sigSize=1
flag=""
files=100

for cores in 20 10 4 #2 1
do
	echo "collect similars s$sigSize f$files c$cores $flag start:" $(date +"%T")
	$SPARK --class de.hpi.fgis.willidennis.Main \
	--conf spark.cores.max=$cores \
	$JAR \
	--TRAINING_PATH $INPUT$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$flag" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores$flag" \
	--CORES $cores \
	> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores$flag"
done

# cores=20
# for files in 200 500 1000
# do
# 	echo "collect similars s$sigSize f$files c$cores $flag start:" $(date +"%T")
# 	$SPARK --class de.hpi.fgis.willidennis.Main \
# 	--conf spark.cores.max=$cores \
# 	$JAR \
# 	--TRAINING_PATH $INPUT$files/ \
# 	--SIGNATURE_SIZE $sigSize --FILES 1 \
# 	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$flag" \
# 	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores$flag" \
# 	--CORES $cores \
# 	--MEMORY "6G" \
# 	> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores$flag"
# done

echo "finish:" $(date +"%T")