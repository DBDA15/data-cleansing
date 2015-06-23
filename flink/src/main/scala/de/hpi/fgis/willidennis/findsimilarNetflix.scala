package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scopt.OptionParser
import org.apache.flink.util.Collector

case class Config(CORES:Int = 1,
									SIM_THRESHOLD:Double = 0.9,
									SIGNATURE_SIZE:Int = 2,
									TRAINING_PATH:String = "netflixdata/training_set",
									FILES:Int = 5,
									LINES:Int = -1)

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

	def generateCandidates(candidates: Array[(Int, Int)] ): Array[(Int,Int)] = {
		val SIMTHRESHOLD = 0.9

		val candLength = candidates.length
		val result = new Array[(Int, Int)]((0.5*(candLength-1)*(candLength)).toInt) // Gauß'sche Summenformel based on candLength-1
		var index = 0
		for(i<-0 to (candLength-2)) {
			val user1 = candidates(i)
			for(n<-(i+1) to (candLength-1)) {
				val user2 = candidates(n)
				// Length-filter
				var sizesInRange = false
				if(user1._2 < user2._2) {
					sizesInRange = user2._2*SIMTHRESHOLD <= user1._2
				} else {
					sizesInRange = user1._2*SIMTHRESHOLD <= user2._2
				}

				if(sizesInRange) {
					result(index) = (user1._1, user2._1)
					index += 1
				}
			}
		}
		return result.filter(_ != null)
	}

	def parseLine(line: String, movid:Int):Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, movid, splitted(1).toInt) // (userid, movid, rating)
	}

	def compareCandidates(candidatesArray: Array[ Array[Rating] ]): Array[(String, Long)] = {
		val SIMTHRESHOLD = 0.9 /* TODO: where else can we set this!? */
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
					sizesInRange = user2.size*SIMTHRESHOLD <= user1.size
				} else {
					sizesInRange = user1.size*SIMTHRESHOLD <= user2.size
				}

				comparisonsRaw += 1

				if(sizesInRange) {
					val simvalue = calculateSimilarity(user1, user2)
					comparisonsEffective += 1
					if(simvalue >= SIMTHRESHOLD) {
						numberOfSims += 1
					}
				}
			}
		}
		return Array(	("similarities", numberOfSims), ("unpruned comparisons", comparisonsRaw),
			("comps after length filter", comparisonsEffective))
	}

	def groupAllUsersRatings(SIMTHRESHOLD:Double, SIGNATURE_SIZE:Int, in: Iterator[Rating], out: Collector[(String, Array[Rating])])  {
		val allRatingsOfUser = in.toArray
		val signatureLength = allRatingsOfUser.size - math.ceil(SIMTHRESHOLD*allRatingsOfUser.size).toInt + SIGNATURE_SIZE

		val sortedRatings = allRatingsOfUser.toArray.sortBy(_.movie)
		val prefix = sortedRatings.slice(0, signatureLength).toList
		val signatures = combinations(prefix, SIGNATURE_SIZE).toArray

		for(sig <- signatures) {
			val longSignature = sig.map((s:Rating) => SignatureKey(s.movie, s.stars))
			val signatureString = longSignature.map(x => x.movie.toString + ',' +x.stars.toString).mkString(";")
			out.collect( (signatureString, allRatingsOfUser))
		}
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
		val parser = new OptionParser[Config]("foo") {
			head("data.cleansing", "0.1")
			opt[String]("TRAINING_PATH") action { (path, c) =>
				c.copy(TRAINING_PATH = path)
			} text ("path of training data set")
			opt[Int]("CORES") action { (n, c) =>
				c.copy(CORES = n)
			} text ("number of cores")
			opt[Int]("FILES") action {(a, c) =>
				c.copy(FILES = a)
			} text("number of files")
			opt[Int]("LINES") action {(a, c) =>
				c.copy(LINES = a)
			} text("first number of lines of input file")
			opt[Double]("SIM_THRESHOLD") action { (s, c) =>
				c.copy(SIM_THRESHOLD = s)
			} text ("jaccard similarity threshold")

			help("help") text ("prints this usage text")
		}
		//run with:
		// $ flink run target/findSimilarNetflix-0.0.1-SNAPSHOT.jar --TRAINING_PATH netflixdata/training_set
		parser.parse(args, new Config) match {
			case Some(config) => run(config)
			case None => // arguments are bad, error message will have been displayed
		}
	}

	def outputStats(similar:DataSet[Array[(String, Long)]]) = {
		val aggregatedStats = similar.reduce {
			(x: Array[(String, Long)], y: Array[(String, Long)]) =>
				Array( (x(0)._1, x(0)._2 + y(0)._2), (x(1)._1, x(1)._2 + y(1)._2), (x(2)._1, x(2)._2 + y(2)._2))
		}
		val printableAggregatedStats = aggregatedStats.map(_.toList)
		printableAggregatedStats.writeAsText("file:///tmp/flink-aggregated-stats", writeMode=FileSystem.WriteMode.OVERWRITE)
	}

	def run(config: Config) {
		val timeAtBeginning = System.currentTimeMillis

		val env = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(config.CORES)
		val mapped = parseFiles(env, config.FILES, config.TRAINING_PATH)

		val users: GroupedDataSet[Rating] = mapped.groupBy("user")
		val signed: DataSet[(String, Array[Rating])] = users.reduceGroup(groupAllUsersRatings(config.SIM_THRESHOLD, config.SIGNATURE_SIZE, _, _))
		//signed.writeAsCsv("file:///tmp/flink-user", writeMode=FileSystem.WriteMode.OVERWRITE)

		val SIGNATURE = 0
		val similar = signed.groupBy(SIGNATURE).reduceGroup {
			(in:  Iterator[ (String, Array[Rating]) ], out: Collector[ Array[(String, Long)] ])  =>
				val buckets = in.map(_._2).toArray
				out.collect(compareCandidates(buckets))
		}
		//similar.writeAsText("file:///tmp/flink-similar", writeMode=FileSystem.WriteMode.OVERWRITE)
		outputStats(similar)
		/*allRatingsOfUser foreach((x: Rating) => out.collect(
				(SignatureKey(x.movie, x.stars), allRatingsOfUser) ))*/

		env.execute("data-cleansing")
		println(s"time: ${System.currentTimeMillis - timeAtBeginning}")
	}
}