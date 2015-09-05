DATA CLEANSING
----
This application uses the Apache Spark/Flink Frameworks to find similar users in the Netflix user data set.
Users are compared based on the movies they rated, the rating value itself gets ignored.

BUILD
----
Use Java 7 and maven package

APACHE SPARK
----
Sample execution:
```
spark-submit --class de.hpi.fgis.willidennis.Main --master spark://172.16.21.111:7077  --conf spark.cores.max=10 scala/target/findSimilarNetflix-0.0.1-SNAPSHOT.jar training_set/ 100 1000
```

PROGRAM ARGUMENTS
----

APACHE FLINK
----
Sample execution:
```
flink run --class de.hpi.fgis.willidennis.Main target/findSimilarNetflix.jar --TRAINING_PATH training_set/by_user/ --SIGNATURE_SIZE 1 --FILES 1
```

OUTPUT
----