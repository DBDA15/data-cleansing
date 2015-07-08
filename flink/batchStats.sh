FLINK="/opt/flink/flink-0.9.0/bin/flink"
JAR="target/findSimilarNetflix-1.0.jar"
INPUT="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
LOG_DIR="out/mylogs"
OUTPUT_DIR="hdfs://tenemhead2/data/data-cleansing/out"
BUILD="1.0"

sigSize=1
cores=20
files=200

for flag in "" --USE_LENGTH_CLASSES_IN_SIG
do
	echo "collect similars s$sigSize f$files c$cores $flag start:" $(date +"%T")
	$FLINK run --class de.hpi.fgis.willidennis.Main \
	$JAR \
	--TRAINING_PATH $INPUT$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$flag" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores$flag" \
	--CORES $cores \
	$flag \
	> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores$flag"
done

flag=""
files=100
for cores in 20 10 4 2 1
do
	echo "collect similars s$sigSize f$files c$cores $flag start:" $(date +"%T")
	$FLINK run --class de.hpi.fgis.willidennis.Main \
	$JAR \
	--TRAINING_PATH $INPUT$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$flag" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores$flag" \
	--CORES $cores \
	> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores$flag"
done

files=500
for flag in "" --USE_LENGTH_CLASSES_IN_SIG
do
	echo "collect similars s$sigSize f$files c$cores $flag start:" $(date +"%T")
	$FLINK run --class de.hpi.fgis.willidennis.Main \
	$JAR \
	--TRAINING_PATH $INPUT$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f${files}c${cores}$flag" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores$flag" \
	--CORES $cores \
	$flag \
	> "$LOG_DIR/log-findSimilars-s${sigSize}f${files}c$cores$flag"
done
echo "finish:" $(date +"%T")
