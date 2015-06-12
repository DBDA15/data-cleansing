package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)
case class NumberOfRatings(user:Int, number:Int)
case class UserRatings(user:Int, ratings:Iterable[Rating])

object Main extends App {

	def determineSignature (user: UserRatings ) : Array[(SignatureKey, Array[NumberOfRatings] )] = {
		val rsize = user.ratings.size
		val result = new Array[(SignatureKey, Array[NumberOfRatings])] (rsize)
		var i = 0
		for ( rat <- user.ratings )  {
			result(i) = ( SignatureKey(rat.movie, rat.stars),
						  Array(NumberOfRatings(user.user, rsize)) ) // Array only to allow concat in following step
			i = i+1
		}
		return result
	}

	def calculateSimilarity(user1: Iterable[Rating], user2: Iterable[Rating]) : Double = {
		val u1set = user1.map(x => (x.movie, x.stars)).toSet
		val u2set = user2.map(x => (x.movie, x.stars)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	/*
	*	out: (Int, Int) = (number of similarities found, number of comparisons, number of comparisons saved)
	*/
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


	override def main(args: Array[String]) = {
		val timeAtBeginning = System.currentTimeMillis
		val SIMTHRESHOLD = 0.9

		var firstNLineOfFile = -1
		var numberOfFiles = 4
		var numberOfMoviesForSig = 2
		var TRAINING_PATH = "netflixdata/training_set"
		var NROFCORES = 1

		if(args.size > 0) {
			TRAINING_PATH = args(0)
		}
		if(args.size > 1) {
			numberOfFiles = args(1).toInt
		}

		if(args.size > 2) {
			firstNLineOfFile = args(2).toInt
		}

		if(args.size > 3) {
			NROFCORES = args(3).toInt
		}

		val env = ExecutionEnvironment.getExecutionEnvironment

		var mapped = env.fromCollection(Array[Rating]())


		/* File input
		*
		*/
		/* split RDD every N files to avoid stackoverflow */
		for(i <- 1 to numberOfFiles) {
			val text = env.readTextFile(TRAINING_PATH + "/mv_" + "%07d".format(i) + ".txt");
			val filtered = text.filter(line => ! line.contains(":"))
			mapped = mapped.union(filtered.map(line => parseLine(line,i)))
		}

		val users = mapped.groupBy("movie")
		// val signatures = users.flatMap(determineSignature) // flatMap only works with a DataSet, use mapPartition?
		// val buckets = signatures.reduceGroup( (a, b) => concat(a,b)).filter(_.size > 1)
		mapped.writeAsText("file:///tmp/flink-log.txt")

		env.execute

	}
}
