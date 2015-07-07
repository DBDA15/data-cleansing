pathToBinFlink="/opt/flink/flink-0.9.0/bin/flink"
pathToJars="target"
MY_TRAINING_PATH="hdfs://tenemhead2/data/data-cleansing/netflixdata/training_set/cat"
LOG_DIR="out/mylogs"
OUTPUT_DIR="out"

for files in 10 100 1000
do
	for sigSize in 1 2
	do
		echo "create Signatures s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main \
		$pathToJars/findSimilarNetflix-377eaf-createSignatures.jar \
		--TRAINING_PATH $MY_TRAINING_PATH/$files/ \
		--SIGNATURE_SIZE $sigSize --FILES 1 \
		--CORES 20
		> "$LOG_DIR/log-createSigs-s$sigSizef$files"

		echo "collect bucketsizes s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main \
		$pathToJars/findSimilarNetflix-377eaf-collectBucketSizes.jar \
		--TRAINING_PATH $MY_TRAINING_PATH/$files/ \
		--SIGNATURE_SIZE $sigSize --FILES 1 \
		--STAT_FILE "$OUTPUT_DIR/bucketSizes-s$sigSize""f$files" \
		--EXECUTION_NAME "data-cleansing-bucketSizes-s($sigSize)f$files" \
		--CORES 20
		> "$LOG_DIR/log-collectBucketSizes-s$sigSize""f$files"

		echo "collect similars s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main \
		$pathToJars/findSimilarNetflix-377eaf-findSimilars.jar \
		--TRAINING_PATH $MY_TRAINING_PATH/$files/ \
		--SIGNATURE_SIZE $sigSize --FILES 1 \
		--OUTPUT_FILE "$OUTPUT_DIR/similars-s$sigSize""f$files" \
		--EXECUTION_NAME "data-cleansing-findSimilars-s($sigSize)f$files" \
		--CORES 20
		> "$LOG_DIR/log-findSimilars-s$sigSize""f$files"
	done
done
echo "finish:" $(date +"%T")
