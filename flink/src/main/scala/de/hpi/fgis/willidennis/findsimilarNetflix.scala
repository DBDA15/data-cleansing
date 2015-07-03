package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scopt.OptionParser
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

case class Config(CORES:Int = 1,
									SIM_THRESHOLD:Double = 0.9,
									SIGNATURE_SIZE:Int = 1,
									TRAINING_PATH:String = "netflixdata/training_set/by_user/",
									FILES:Int = 5,
									LINES:Int = -1,
									STAT_FILE:String = "file:///tmp/flink-aggregated-stats",
									OUTPUT_FILE:String = "file:///tmp/flink-output",
									EXECUTION_NAME:String = "data-cleansing")

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)
case class NumberOfRatingsPerUser(user:Int, number:Int)
case class UserRatings(user:Int, ratings:Iterable[Rating])

object Main extends App {

	def combinations[T](aList:List[T], n:Int) : Iterator[List[T]] = {
		if(aList.length < n) return Iterator(aList) // aList.combinations(n) would be an empty List
		return aList.combinations(n)
	}

	def parseLine(line: String):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, splitted(1).toInt, splitted(2).toInt)
	}

	def groupAllUsersRatings(SIMTHRESHOLD:Double, SIGNATURE_SIZE:Int, movieMap: Map[Int, Int], in: Iterator[Rating], out: Collector[(String, Int)])  {
		val allRatingsOfUser = in.toArray
		val prefixLength = allRatingsOfUser.size - math.ceil(SIMTHRESHOLD*allRatingsOfUser.size).toInt + SIGNATURE_SIZE
		val userID = allRatingsOfUser(0).user

		// find out the n (signatureLength) rated movies with the least ratings

		val sortedRatings = allRatingsOfUser.sortBy(rating => movieMap.get(rating.movie).get)
		val prefix = sortedRatings.slice(0, prefixLength).toList

		val signatures = combinations(prefix.toList, SIGNATURE_SIZE).toArray

		for(sig <- signatures) {
			val longSignature = sig.map((s:Rating) => SignatureKey(s.movie, s.stars))
			val signatureString = longSignature.map(x => x.movie.toString + '-' +x.stars.toString).mkString(";")
			out.collect( (signatureString, userID))
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
			opt[Int]("LINES") action { (a, c) =>
				c.copy(LINES = a)
			} text ("first number of lines of input file")
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

			help("help") text ("prints this usage text")
		}
		//run with:
		// $ flink run target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --TRAINING_PATH netflixdata/training_set
		parser.parse(args, new Config) match {
			case Some(config) => run(config)
			case None => // arguments are bad, error message will have been displayed
		}
	}


	def run(config: Config) {
		val timeAtBeginning = System.currentTimeMillis

		//val nrIterations = new LongCounter()
		//env.getRuntimeContext().addAcc

		val env = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(config.CORES)
		val mapped = parseFiles(config, env)

		val numberOfRatingsPerMovie = mapped.groupBy("movie").reduceGroup {
			(in:  Iterator[ Rating ], out: Collector[ (Int, Int) ])  =>
				val ratingsList = in.toList
				val statisticsEntry = (ratingsList(0).movie, ratingsList.size)
				out.collect(statisticsEntry)
		}

		val movieMap = numberOfRatingsPerMovie.collect.map(x => x._1 -> x._2).toMap

		val users: GroupedDataSet[Rating] = mapped.groupBy("user")
		val signed: DataSet[(String, Int)] = users.reduceGroup(groupAllUsersRatings(config.SIM_THRESHOLD, config.SIGNATURE_SIZE, movieMap, _, _))
		//signed.writeAsCsv("file:///tmp/flink-user", writeMode=FileSystem.WriteMode.OVERWRITE)

		val SIGNATURE = 0
		val bucketSizes = signed.groupBy(SIGNATURE).reduceGroup {
			(in:  Iterator[ (String, Int) ], out: Collector[ (String, Int) ])  =>
				val all = in.toList
				val signature = all(0)._1
				out.collect((signature, all.length))
		}

		bucketSizes.filter(_._2>1).writeAsCsv(config.OUTPUT_FILE, writeMode=FileSystem.WriteMode.OVERWRITE)
		//println(env.getExecutionPlan())
		env.execute(config.EXECUTION_NAME)
		println(s"time: ${System.currentTimeMillis - timeAtBeginning}")
	}
}
