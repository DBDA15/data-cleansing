pathToBinFlink="../../flink-0.9.0/bin/flink"
pathToJars="target"
MY_TRAINING_PATH="../netflixdata/training_set/by_user"
#do not write to tmp!
LOG_DIR="/tmp/mylogs"
OUTPUT_DIR="/tmp/flinkout"

for files in 10 100 17770
do
	for sigSize in 1 2
	do
		echo "create Signatures s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main $pathToJars/findSimilarNetflix-377eaf-createSignatures.jar --TRAINING_PATH $MY_TRAINING_PATH/$files/ --SIGNATURE_SIZE $sigSize --FILES $files > "$LOG_DIR/log-createSigs-s$sigSizef$files"

		echo "collect bucketsizes s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main $pathToJars/findSimilarNetflix-377eaf-collectBucketSizes.jar --TRAINING_PATH $MY_TRAINING_PATH/$files/ --SIGNATURE_SIZE $sigSize --FILES $files --STAT_FILE "$OUTPUT_DIR/bucketSizes-s$sigSize""f$files" --EXECUTION_NAME "data-cleansing-bucketSizes-s($sigSize)f$files" > "$LOG_DIR/log-collectBucketSizes-s$sigSize""f$files"

		echo "collect similars s$sigSize f$files start:" $(date +"%T")
		$pathToBinFlink run --class de.hpi.fgis.willidennis.Main $pathToJars/findSimilarNetflix-377eaf-findSimilars.jar --TRAINING_PATH $MY_TRAINING_PATH/$files/ --SIGNATURE_SIZE $sigSize --FILES $files --OUTPUT_FILE "$OUTPUT_DIR/similars-s$sigSize""f$files" --EXECUTION_NAME "data-cleansing-findSimilars-s($sigSize)f$files" > "$LOG_DIR/log-findSimilars-s$sigSize""f$files"
	done
done
echo "finish:" $(date +"%T")
