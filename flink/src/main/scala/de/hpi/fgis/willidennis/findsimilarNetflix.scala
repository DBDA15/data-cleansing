package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scopt.OptionParser
import org.apache.flink.util.Collector

case class Config(CORES:Int = 1,
									SIM_THRESHOLD:Double = 0.9,
									SIGNATURE_SIZE:Int = 1,
									TRAINING_PATH:String = "netflixdata/training_set/by_user/",
									FILES:Int = 5,
									LINES:Int = -1,
									STAT_FILE:String = "file:///tmp/flink-ratings-per-user",
									OUTPUT_FILE:String = "file:///tmp/flink-output",
									OUTPUT_FILE:String = "file:///tmp/flink-output",
									EXECUTION_NAME:String = "data-cleansing")

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)
case class NumberOfRatingsPerUser(user:Int, number:Int)
case class UserRatings(user:Int, ratings:Iterable[Rating])

object Main extends App {

	def parseLine(line: String):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, splitted(1).toInt, splitted(2).toInt)
	}

	def groupByUser(in: Iterator[Rating], out: Collector[(Int, Int)])  {
		val allRatingsOfUser = in.toArray
		out.collect(allRatingsOfUser(0).user, allRatingsOfUser.size)
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

		val env = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(config.CORES)
		val mapped = parseFiles(config, env)

		val users: DataSet[(Int, Int)] = mapped.groupBy("user").reduceGroup(groupByUser(_, _))
		//signed.writeAsCsv("file:///tmp/flink-user", writeMode=FileSystem.WriteMode.OVERWRITE)

		
		users.writeAsCsv(config.STAT_FILE, writeMode=FileSystem.WriteMode.OVERWRITE)

		env.execute(config.EXECUTION_NAME)
		println(s"time: ${System.currentTimeMillis - timeAtBeginning}")
	}
}
