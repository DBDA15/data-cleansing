#!/usr/bin/env bash
FLINK="/opt/flink/flink-0.9.0/bin/flink"
JAR="flink/target/findSimilarNetflix-1.0.jar"
INPUT="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
LOG_DIR="flink/out/mylogs"
OUTPUT_DIR="hdfs://tenemhead2/data/data-cleansing/out"
BUILD="1.0"

sigSize=1
files=100
for cores in 20 10 4 2 1
do
	for i in {1..4}
	do
		echo "collect similars s$sigSize f$files c${cores} i${i} start:" $(date +"%T")
		$FLINK run --class de.hpi.fgis.willidennis.Main \
		$JAR \
		--TRAINING_PATH $INPUT$files/ \
		--SIGNATURE_SIZE $sigSize --FILES 1 \
		--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$i{i}" \
		--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c${cores}i${i}" \
		--CORES $cores \
		> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c${cores}i${i}"
	done
done