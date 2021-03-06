package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import scopt.OptionParser
import scala.collection.mutable.ArrayBuffer

case class Config(	CORES:Int = 1,
					SIM_THRESHOLD:Double = 0.9,
					SIGNATURE_SIZE:Int = 1,
					TRAINING_PATH:String = "../netflixdata/training_set/by_user/",
					FILES:Int = 5,
					STAT_FILE:String = "file:///tmp/flink-aggregated-stats",
					OUTPUT_FILE:String = "file:///tmp/flink-output",
					EXECUTION_NAME:String = "data-cleansing",
					USE_LENGTH_CLASSES_IN_SIG:Boolean = false
				)

case class Rating(user:Int, movie:Int)

object Main extends App {

	def combinations[T](aList:List[T], n:Int) : Iterator[List[T]] = {
		if(aList.length < n) return Iterator(aList) // aList.combinations(n) would be an empty List
		return aList.combinations(n)
	}

	def calculateSimilarity(user1: Iterable[Rating], user2: Iterable[Rating]) : Double = {
		val u1set = user1.map(x => x.movie).toSet
		val u2set = user2.map(x => x.movie).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}


	def parseLine(line: String):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, splitted(1).toInt)
	}

	def compareCandidates(config:Config, candidatesArray: Array[ Array[Rating] ], out: Collector[(Int, Int)])= {
		for(i<-0 to (candidatesArray.length-2)) {
			var user1 = candidatesArray(i)

			/* compare with all elements that follow */
			for(n<-(i+1) to (candidatesArray.length-1)) {
				var user2 = candidatesArray(n)

				/* calculate similarity and add to result if sizes are close enough (depends on SIMTHRESHOLD) */
				val sizesInRange = config.SIM_THRESHOLD * math.max(user1.size, user2.size) <= math.min(user1.size, user2.size)
				if(sizesInRange) {
					val simvalue = calculateSimilarity(user1, user2)
					if(simvalue >= config.SIM_THRESHOLD) {
						out.collect((user1.head.user, user2.head.user))
					}
				}
			}
		}
	}

	def getLengthClasses(SIMTHRESHOLD:Double, numberOfRatings:Int): ArrayBuffer[Int] = {
		val lengthClasses = ArrayBuffer[Int]()
		var classBefore = 0
		var thisClass = 0
		do {
			classBefore = thisClass
			// TODO: does the lengthClass also have to do with the sigSize? (like the prefix does)
			val classSize = classBefore - math.ceil(SIMTHRESHOLD*classBefore).toInt + 1
			thisClass = classBefore + classSize
		} while (numberOfRatings >= 2*thisClass - math.ceil(SIMTHRESHOLD*thisClass).toInt + 1);
		lengthClasses.append(thisClass)
		if(math.ceil(numberOfRatings*SIMTHRESHOLD)<thisClass) {
			lengthClasses.append(classBefore)
		}
		return lengthClasses
	}

	def createSignature(config: Config, movieMap: Map[Int, Int], in: Iterator[Rating], out: Collector[(String, Int)])  {
		val SIMTHRESHOLD = config.SIM_THRESHOLD
		val SIGNATURE_SIZE = config.SIGNATURE_SIZE

		val allRatingsOfUser = in.toArray
		val prefixLength = allRatingsOfUser.size - math.ceil(SIMTHRESHOLD*allRatingsOfUser.size).toInt + SIGNATURE_SIZE
		val userID = allRatingsOfUser(0).user

		// find out the n (signatureLength) rated movies with the least ratings
		val sortedRatings = allRatingsOfUser.sortBy(_.movie).sortBy(rating => movieMap.get(rating.movie).get)
		//val sortedRatings = allRatingsOfUser.sortBy(_.movie)
		val prefix = sortedRatings.slice(0, prefixLength)

		val signatures = combinations(prefix.toList, SIGNATURE_SIZE).toArray

		for(sig <- signatures) {
			if(config.USE_LENGTH_CLASSES_IN_SIG) {
				for(lengthClass <- getLengthClasses(SIMTHRESHOLD, allRatingsOfUser.size)) {
					val signatureString = lengthClass + "_" + sig.map((x:Rating) => x.movie.toString).mkString(";")
					out.collect( (signatureString, userID))
				}
			}
			else {
				val signatureString = sig.map((x:Rating) => x.movie.toString).mkString(";")
				out.collect( (signatureString, userID))
			}
		}
	}

	def parseFiles(config:Config, env: ExecutionEnvironment): DataSet[Rating] = {
		var mapped: DataSet[Rating] = env.fromCollection(Array[Rating]())

		for(i <- 0 to config.FILES - 1) {
			var text = env.readTextFile(config.TRAINING_PATH + s"${i}.csv")

			mapped = mapped.union(text.map(line => parseLine(line)))
		}
		return mapped
	}

	def collectBucketSize(dataSet: DataSet[(String, Int)] ): DataSet[(String, Int)] = {
		val SIGNATURE = 0
		dataSet.groupBy(SIGNATURE).reduceGroup {
			(candidates: Iterator[(String, Int)], out: Collector[(String, Int)]) =>
				val signature = candidates.next._1
				val candidatesInBucket = candidates.size + 1 		// already fetched first element
				if(candidatesInBucket > 1)
					out.collect((signature, candidatesInBucket))
		}.name("collect bucket size")
	}

	def collectMovieStats(dataSet: DataSet[Rating]): Map[Int, Int] = {
		val numberOfRatingsPerMovie = dataSet.groupBy("movie").reduceGroup {
			(in:  Iterator[ Rating ], out: Collector[ (Int, Int) ])  =>
				val movieID = in.next.movie
				val numberOfRatings = in.size + 1
				out.collect(movieID -> numberOfRatings)
		}.name("movie stats")
		numberOfRatingsPerMovie.collect.toMap
	}

	def getUserData(users: GroupedDataSet[Rating]): DataSet[(Int, Array[Rating])] = {
		users.reduceGroup {
			(in:  Iterator[Rating], out: Collector[ (Int, Array[Rating]) ])  =>
				val allRatings = in.toArray
				out.collect((allRatings.head.user, allRatings))
		}.name("collect user ratings")
	}

	def cleanAndFlattenBuckets(dataSet: DataSet[(String, Int)]): DataSet[(String, Int)] = {
		val SIGNATURE = 0
		dataSet.groupBy(SIGNATURE).reduceGroup {
			(in:  Iterator[(String,Int)], out: Collector[ (String, Int) ]) =>
				val userIdBucket = in.toList
				if(hasPairsInBucket(userIdBucket))
					userIdBucket.foreach(aUser => out.collect(aUser))
		}.name("clean and flatten buckets")
	}

	def joinCandidatesWithRatings(candidateBuckets: DataSet[(String, Int)],
																userData: DataSet[(Int, Array[Rating])]): DataSet[(String, Array[Rating])] = {
		val BUCKET_USER_ID = 1
		val USER_DATA_USER_ID = 0
		candidateBuckets.joinWithHuge(userData).where(BUCKET_USER_ID).equalTo(USER_DATA_USER_ID).map(x => (x._1._1, x._2._2)).name("join candidates ⋈ ratings")
	}

	def similaritiesInBuckets(candidates:DataSet[(String, Array[Rating])], config: Config): DataSet[(Int, Int)] = {
		val SIGNATURE = 0
		val similar = candidates.groupBy(SIGNATURE).reduceGroup {
			(in:  Iterator[(String, Array[Rating])], out: Collector[ (Int, Int) ])  =>
				val bucket = in.map(_._2).toArray
				compareCandidates(config, bucket, out)
		}.name("similarities in buckets")
		return similar
	}

	override def main(args: Array[String]) = {
		val parser = new OptionParser[Config]("find similar") {
			head("data.cleansing", "0.1")
			opt[String]("TRAINING_PATH") action { (path, c) =>
				c.copy(TRAINING_PATH = path)
			} text ("path of training data set")
			opt[Int]("CORES") action { (n, c) =>
				c.copy(CORES = n)
			} text ("number of cores")
			opt[Int]("FILES") action { (a, c) =>
				c.copy(FILES = a)
			} text ("number of files")
			opt[Int]("SIGNATURE_SIZE") action { (s, c) =>
				c.copy(SIGNATURE_SIZE = s)
			} text ("sig size")
			opt[Double]("SIM_THRESHOLD") action { (s, c) =>
				c.copy(SIM_THRESHOLD = s)
			} text ("jaccard similarity threshold")
			opt[String]("STAT_FILE") action { (s, c) =>
				c.copy(STAT_FILE = s)
			} text ("file for stats printing")
			opt[String]("OUTPUT_FILE") action { (s, c) =>
				c.copy(OUTPUT_FILE = s)
			} text ("file for stats printing")
			opt[String]("EXECUTION_NAME") action { (s, c) =>
				c.copy(EXECUTION_NAME = s)
			} text ("Name of this execution")
			opt[Unit]("USE_LENGTH_CLASSES_IN_SIG") action { (_, c) =>
    			c.copy(USE_LENGTH_CLASSES_IN_SIG = true) } text("verbose is a flag")

			help("help") text ("prints this usage text")
		}
		//run with:
		// $ flink run target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --TRAINING_PATH netflixdata/training_set
		parser.parse(args, new Config) match {
			case Some(config) => run(config)
			case None => // arguments are bad, error message will have been displayed
		}
	}

	def outputStats(config:Config, similar:DataSet[Array[(String, Long)]]) = {
		val aggregatedStats = similar.reduce {
			(x: Array[(String, Long)], y: Array[(String, Long)]) =>
				Array( (x(0)._1, x(0)._2 + y(0)._2), (x(1)._1, x(1)._2 + y(1)._2), (x(2)._1, x(2)._2 + y(2)._2))
		}
		val printableAggregatedStats = aggregatedStats.map(_.toList)
		printableAggregatedStats.writeAsText(config.STAT_FILE, writeMode=FileSystem.WriteMode.OVERWRITE)
	}

	def hasPairsInBucket(bucket: List[(String,Int)]): Boolean = {
		bucket.size > 1
	}

	def ratingsPerMovieHistogram(movieStats: Map[Int, Int], config: Config, env: ExecutionEnvironment) = {
		val moviesByNRatings = movieStats.groupBy(_._2)
		val histogram = moviesByNRatings.map {
			x =>
				val nRatings = x._1
				val nMovies = x._2.size
				(nRatings, nMovies)
		}.toList.sortBy(_._1)
		env.fromCollection(histogram).writeAsCsv(config.OUTPUT_FILE, writeMode = FileSystem.WriteMode.OVERWRITE)
	}

	def run(config: Config) {
		val timeAtBeginning = System.currentTimeMillis

		val env = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(config.CORES)
		val mapped = parseFiles(config, env)

		val movieStats = collectMovieStats(mapped)
		//ratingsPerMovieHistogram(movieStats, config, env)

		val users: GroupedDataSet[Rating] = mapped.groupBy("user")
		val userData = getUserData(users)

		val signed: DataSet[(String, Int)] = users.reduceGroup(createSignature(config, movieStats, _, _))

		//val bucketSizes = collectBucketSize(signed)
		//bucketSizes.writeAsCsv(config.STAT_FILE, writeMode = FileSystem.WriteMode.OVERWRITE).name("write bucket sizes")

		val cleanFlatBuckets = cleanAndFlattenBuckets(signed)
		val candidatesWithRatings = joinCandidatesWithRatings(cleanFlatBuckets, userData)

		val similar = similaritiesInBuckets(candidatesWithRatings, config)

		similar.writeAsCsv(config.OUTPUT_FILE, writeMode=FileSystem.WriteMode.OVERWRITE).name("write similarities")
		//println(env.getExecutionPlan())
		//outputStats(config, similar)

		env.execute(config.EXECUTION_NAME)

		//System.err.println(s"#### signatures: ${signed.count}")
		System.err.println(s"#### time: ${System.currentTimeMillis - timeAtBeginning}")
	}
}
