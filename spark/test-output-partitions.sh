#!/usr/bin/env bash
SPARK="/opt/spark/spark-1.3.0/bin/spark-submit"
# SPARK=spark-submit
JARS="target/findSimilarNetflix-1.0-repartition-before-cc.jar" "target/findSimilarNetflix-1.0-minPartitions.jar" "target/findSimilarNetflix-1.0-repartition-after-read.jar" "target/findSimilarNetflix-1.0-repartition-before-join.jar"
INPUT="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
# INPUT="../netflixdata/cat"
LOG_DIR="out/mylogs/spark"
# LOG_DIR="."
OUTPUT_DIR="hdfs://tenemhead2/data/data-cleansing/spark/out"
# OUTPUT_DIR="/tmp/spark-output"
BUILD="1.0"
# MASTER="localhost:4041"
LOGFILE="$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores"

sigSize=1
files=10
cores=20

for JAR in $JARS
do
	echo "collect similars s$sigSize f$files c$cores  start:" $(date +"%T")
	$SPARK --class de.hpi.fgis.willidennis.Main \
	--conf spark.cores.max=$cores \
	$JAR \
	--TRAINING_PATH $INPUT$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores" \
	--CORES $cores \
	> $LOGFILE
	echo $(grep "output partitions" ${LOGFILE})
done
echo "finish:" $(date +"%T")