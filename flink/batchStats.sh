pathToBinFlink="/opt/flink/flink-0.9.0/bin/flink"
pathToJars="target"
MY_TRAINING_PATH="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
LOG_DIR="out/mylogs"
OUTPUT_DIR="hdfs://tenemhead2/data/data-cleansing/out"
BUILD="1.0"

sigSize=1
cores=20

$pathToBinFlink run --class de.hpi.fgis.willidennis.Main \
		$pathToJars/findSimilarNetflix-$BUILD.jar \
		--TRAINING_PATH $MY_TRAINING_PATH$files/ \
		--SIGNATURE_SIZE $sigSize --FILES 1 \
		--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f$files$cores" \
		--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores" \
		--CORES $cores \
		--USE_LENGTH_CLASSES_IN_SIG \
		> "$LOG_DIR/log-findSimilars-s$sigSize$files$cores"

for cores in 20 10 4 2 1
do
	echo "collect similars s$sigSize f$files c$cores start:" $(date +"%T")
	$pathToBinFlink run --class de.hpi.fgis.willidennis.Main \
	$pathToJars/findSimilarNetflix-377eaf-findSimilars.jar \
	--TRAINING_PATH $MY_TRAINING_PATH$files/ \
	--SIGNATURE_SIZE $sigSize --FILES 1 \
	--OUTPUT_FILE "$OUTPUT_DIR/similars-s${sigSize}f$files$cores" \
	--EXECUTION_NAME "data-cleansing-findSimilars-s${sigSize}f${files}c$cores" \
	--CORES $cores \
	--USE_LENGTH_CLASSES_IN_SIG \
	> "$LOG_DIR/log-findSimilars-s$sigSize$files$cores"
done
echo "finish:" $(date +"%T")
