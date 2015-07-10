package de.hpi.fgis.willidennis

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

case class Rating(user:Int, movie:Int)
case class SignatureKey(movie:Int, stars:Int)

case class Config(	CORES:Int = 1,
										SIM_THRESHOLD:Double = 0.9,
										SIGNATURE_SIZE:Int = 1,
										TRAINING_PATH:String = "../netflixdata/training_set/by_user/",
										FILES:Int = 5,
										STAT_FILE:String = "file:///tmp/spark-aggregated-stats",
										OUTPUT_FILE:String = "file:///tmp/spark-output",
										EXECUTION_NAME:String = "data-cleansing",
										USE_LENGTH_CLASSES_IN_SIG:Boolean = false,
								 		MEMORY:String = "4g",
										MASTER:String = "local[*]",	// use all cores, this overrides --CORES argument
										MEASURE_STUFF:Boolean = false
									 )

object Main extends App {

	////////////////////////////
	// SIGNATURE
	///////////////////////////
	def createSignature(config: Config, movieMap: Map[Int, Int], allRatingsOfUser: Array[Rating]): ArrayBuffer[(Int, String)] = {
		val out = ArrayBuffer[(Int, String)]()	//Int, String to make it joinable in spark on userID:Int
		val SIMTHRESHOLD = config.SIM_THRESHOLD
		val SIGNATURE_SIZE = config.SIGNATURE_SIZE

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
					out.append( (userID, signatureString))
				}
			}
			else {
				val signatureString = sig.map((x:Rating) => x.movie.toString).mkString(";")
				out.append( (userID, signatureString))
			}
		}
		return out
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

	def combinations[T](aList:List[T], n:Int) : Iterator[List[T]] = {
		if(aList.length < n) return Iterator(aList) // aList.combinations(n) would be an empty List
		return aList.combinations(n)
	}

	def collectMovieStats(ratings: RDD[Rating]): Map[Int, Int] = {
		val ratingsPerMovie = ratings.groupBy(aRating => aRating.movie)
		val numberOfRatingsPerMovie = ratingsPerMovie.map(x => (x._1 -> x._2.size))
		numberOfRatingsPerMovie.collectAsMap.toMap
	}

	////////////////////////////
	// JOIN IN BUCKETS
	///////////////////////////
	def similarities(config: Config,
									 flattenedBuckets: RDD[(String, Iterable[Rating])],
									 comparisonsCounter: Accumulator[Long]): RDD[(Int, Int)] = {
		// group by signature String
		val bucketsBySignature = flattenedBuckets.groupBy(aSignatureUserPair => aSignatureUserPair._1).map(_._2)
		// compare candidates for each group
		bucketsBySignature.flatMap {
			aBucket =>
				val candidates = aBucket.map(_._2.toArray).toArray
				compareCandidates(config, candidates, comparisonsCounter)
		}
	}

	def compareCandidates(config:Config, candidates:Array[Array[Rating]], comparisonsCounter: Accumulator[Long]): ArrayBuffer[(Int, Int)] = {
		var comparisonsEffective = 0L

		val result = ArrayBuffer[(Int, Int)]()
		for(i<-0 to (candidates.length-2)) {
			val user1 = candidates(i)

			/* compare with all elements that follow */
			for(n<-(i+1) to (candidates.length-1)) {
				val user2 = candidates(n)
				if(lengthFilter(user1.size, user2.size, config.SIM_THRESHOLD)) {
					val simvalue = calculateSimilarity(user1, user2)
					comparisonsEffective += 1
					if(simvalue >= config.SIM_THRESHOLD) {
						result.append((math.min(user1.head.user, user2.head.user), math.max(user1.head.user, user2.head.user)))
					}
				}
			}
		}
		if(config.MEASURE_STUFF) {
			comparisonsCounter += comparisonsEffective
		}
		return result
	}

	def lengthFilter(size1: Int, size2: Int, threshold:Double): Boolean = {
		return math.max(size1, size2)*threshold <= math.min(size1, size2)
	}

	def calculateSimilarity(user1: Iterable[Rating], user2: Iterable[Rating]) : Double = {
		val u1set = user1.map(x => x.movie).toSet
		val u2set = user2.map(x => x.movie).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	////////////////////////
	// INPUT
	///////////////////////
	def parseLine(line: String):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, splitted(1).toInt)
	}

	def parseFiles(config:Config, sc:SparkContext): RDD[Rating] = {
		var mapped: RDD[Rating] = sc.parallelize(Array[Rating]())

		for(i <- 0 to config.FILES - 1) {
			var text = sc.textFile(config.TRAINING_PATH + s"${i}.csv")
			mapped = mapped.union(text.map(line => parseLine(line)))
		}
		return mapped
	}

	def cleanAndFlattenBuckets(signed: RDD[(Int, String)]): RDD[(Int, String)] = {
		val groupedUsersBySignature = signed.groupBy(_._2)
		val buckets = groupedUsersBySignature.filter(x => x._2.size > 1)
		buckets.flatMap(x => x._2)
	}

	def joinCandidatesWithRatings(signedUsers: RDD[(Int, String)],
																userData: RDD[(Int, Iterable[Rating])]): RDD[(String, Iterable[Rating])] = {
		// (K, V).join(K, W) => (K, (V, W))
		signedUsers.join(userData).map((x:(Int, (String, Iterable[Rating]))) => (x._2._1, x._2._2))
	}

	def deleteOutPutFileIfExists(path: String) = {
		// from http://stackoverflow.com/a/27101351/1510622
		val hadoopConf = new org.apache.hadoop.conf.Configuration()
		val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
		try { hdfs.delete(new org.apache.hadoop.fs.Path(path), true)}
		catch { case _ : Throwable => { } }
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
			opt[String]("MEMORY") action { (s, c) =>
				c.copy(MEMORY = s)
			} text ("Spark Worker Memory")
			opt[String]("EXECUTION_NAME") action { (s, c) =>
				c.copy(EXECUTION_NAME = s)
			} text ("Name of this execution")
			opt[String]("MASTER") action { (s, c) =>
				c.copy(MASTER = s)
			} text ("Spark cluster master")
			opt[Unit]("MEASURE_STUFF") action { (_, c) =>
				c.copy(MEASURE_STUFF = true)
			} text ("flag to perform measurements not done with Flink")
			opt[Unit]("USE_LENGTH_CLASSES_IN_SIG") action { (_, c) =>
				c.copy(USE_LENGTH_CLASSES_IN_SIG = true) } text("is a flag")

			help("help") text ("prints this usage text")
		}
		parser.parse(args, new Config) match {
			case Some(config) => run(config)
			case None => // arguments are bad, error message will have been displayed
		}
	}

	def run(config: Config) = {
		val timeAtBeginning = System.currentTimeMillis

		val sc: SparkContext = configureSpark(config)

		val ratings = parseFiles(config, sc)
		val movieStats = collectMovieStats(ratings)
		val users = ratings.groupBy(_.user)

		val signed = users.flatMap(x => createSignature(config, movieStats, x._2.toArray))
		val buckets = cleanAndFlattenBuckets(signed)

		val comparisonsCounter = sc.accumulator(0L, "Number of comparisons made")
		val candidatesWithRatings = joinCandidatesWithRatings(buckets, users)

		val similar = similarities(config, candidatesWithRatings, comparisonsCounter)
		// similar.distinct() // TODO @Dennis: I would accept duplicates because we have them in our flink measurements too
		if(config.MEASURE_STUFF) {
			similar.cache()
			val simcount = similar.count
			println(s"\n ####### Similarities before duplicate removal: ${simcount}, took ${(System.currentTimeMillis-timeAtBeginning)/1000}s ###### \n")

			val noduplicates = similar.distinct()
			val nodupcount = noduplicates.count
			println(s"\n ####### Similarities after duplicate removal: ${nodupcount} ###### \n\n")
			println(s"\n ####### Duplicates: ${1-(nodupcount/simcount)}%###### \n\n")
			println(s"\n ####### Comparisons : ${comparisonsCounter} #######")
		}
		deleteOutPutFileIfExists(config.OUTPUT_FILE)
		similar.saveAsTextFile(config.OUTPUT_FILE)
		//println(similar.take(10).toList)

		System.err.println(s"#### time: ${System.currentTimeMillis - timeAtBeginning}")
	}

	def configureSpark(conf: Config): SparkContext = {
		var sparkConf = new SparkConf()
		sparkConf.setAppName(conf.EXECUTION_NAME)
		sparkConf.set("spark.executor.memory", conf.MEMORY)
		sparkConf.set("spark.cores.max", conf.CORES.toString)
		sparkConf.setMaster(conf.MASTER)
		new SparkContext(sparkConf)
	}
}
