package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scopt.OptionParser
import org.apache.flink.util.Collector

case class Config(CORES:Int = 1,
									SIM_THRESHOLD:Double = 0.9,
									SIGNATURE_SIZE:Int = 1,
									TRAINING_PATH:String = "netflixdata/training_set",
									FILES:Int = 5,
									LINES:Int = -1,
									STAT_FILE:String = "file:///tmp/flink-aggregated-stats")

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)
case class NumberOfRatingsPerUser(user:Int, number:Int)
case class UserRatings(user:Int, ratings:Iterable[Rating])

object Main extends App {

	def combinations[T](aList:List[T], n:Int) : Iterator[List[T]] = {
		if(aList.length < n) return Iterator(aList) // aList.combinations(n) would be an empty List
		return aList.combinations(n)
	}

	def calculateSimilarity(user1: Iterable[Rating], user2: Iterable[Rating]) : Double = {
		val u1set = user1.map(x => (x.movie, x.stars)).toSet
		val u2set = user2.map(x => (x.movie, x.stars)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}


	def parseLine(line: String, movid:Int):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, movid, splitted(1).toInt) // (userid, movid, rating)
	}

	def compareCandidates(config:Config, candidatesArray: Array[ Array[Rating] ]): Array[(String, Long)] = {
		var numberOfSims = 0.toLong
		var comparisonsRaw = 0.toLong
		var comparisonsEffective = 0.toLong
		
		for(i<-0 to (candidatesArray.length-2)) {
			var user1 = candidatesArray(i)

			/* compare with all elements that follow */
			for(n<-(i+1) to (candidatesArray.length-1)) {
				var user2 = candidatesArray(n)

				/* calculate similarity and add to result if sizes are close enough (depends on SIMTHRESHOLD) */
				var sizesInRange = false
				if(user1.size<user2.size) {
					sizesInRange = user2.size*config.SIM_THRESHOLD <= user1.size
				} else {
					sizesInRange = user1.size*config.SIM_THRESHOLD <= user2.size
				}

				comparisonsRaw += 1

				if(sizesInRange) {
					val simvalue = calculateSimilarity(user1, user2)
					comparisonsEffective += 1
					if(simvalue >= config.SIM_THRESHOLD) {
						numberOfSims += 1
					}
				}
			}
		}
		return Array(	("similarities", numberOfSims), ("unpruned comparisons", comparisonsRaw),
			("comps after length filter", comparisonsEffective))
	}

	def groupAllUsersRatings(in: Iterator[Rating], out: Collector[(Int, List[Rating])])  {
		val allRatingsOfUser = in.toList
		out.collect((allRatingsOfUser.head.user, allRatingsOfUser))
	}

	def parseFiles(env: ExecutionEnvironment, numberOfFiles: Int, TRAINING_PATH: String): DataSet[Rating]  = {
		var mapped: DataSet[Rating] = env.fromCollection(Array[Rating]())

		for(i <- 1 to numberOfFiles) {
			val text = env.readTextFile(TRAINING_PATH + "/mv_" + "%07d".format(i) + ".txt");
			val filtered = text.filter(line => ! line.contains(":"))
			mapped = mapped.union(filtered.map(line => parseLine(line,i)))
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

	def run(config: Config) {
		val timeAtBeginning = System.currentTimeMillis

		val env = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(config.CORES)
		val mapped = parseFiles(env, config.FILES, config.TRAINING_PATH)

		val users = mapped.groupBy("user").reduceGroup(groupAllUsersRatings _)
		users.writeAsCsv("file:///tmp/flink-user", writeMode=FileSystem.WriteMode.OVERWRITE)

		env.execute("data-cleansing")
		println(s"time: ${System.currentTimeMillis - timeAtBeginning}")
	}
}
