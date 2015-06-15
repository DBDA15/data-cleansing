package de.hpi.fgis.willidennis

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import Array._

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)
case class NumberOfRatingsPerUser(user:Int, number:Int)
case class UserRatings(user:Int, ratings:Iterable[Rating])

object Main extends App {

	def determineSignature (ratings: Iterator[Rating], out: Collector[(SignatureKey, Array[NumberOfRatingsPerUser]) ] ) {
		val rsize = ratings.length
		for ( rat <- ratings )  {
			println("collecting!")
			out.collect( (SignatureKey(rat.movie, rat.stars),
						  Array(NumberOfRatingsPerUser(rat.user, rsize)) ) ) // Array only to allow concat in following step
		}
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

	/*
	 	* 	 candidates: Array[ All Ratings of a User ])
    *    out: (Int, Int) = (number of similarities found, number of comparisons, number of comparisons saved)
    */
	def compareCandidates(candidates: Array[ Iterable[Rating] ]): Array[(String, Long)] = {
		val SIMTHRESHOLD = 0.9 /* TODO: where else can we set this!? */
		var numberOfSims = 0.toLong
		var comparisonsRaw = 0.toLong
		var comparisonsEffective = 0.toLong

		for(i<-0 to (candidates.length-2)) {
			var user1 = candidates(i)

			/* compare with all elements that follow */
			for(n<-(i+1) to (candidates.length-1)) {
				var user2 = candidates(n)

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
		return Array(("similarities",numberOfSims), ("unpruned comparisons",comparisonsRaw), ("comps after length filter",comparisonsEffective))
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

		var mapped: DataSet[Rating] = env.fromCollection(Array[Rating]())


		/* File input
		*
		*/
		/* split RDD every N files to avoid stackoverflow */
		for(i <- 1 to numberOfFiles) {
			val text = env.readTextFile(TRAINING_PATH + "/mv_" + "%07d".format(i) + ".txt");
			val filtered = text.filter(line => ! line.contains(":"))
			mapped = mapped.union(filtered.map(line => parseLine(line,i)))
		}

		val users: GroupedDataSet[Rating] = mapped.groupBy("user")

		// TODO: unfortunately I couldnt get this code to work (resulting dataset is empty):
		//val signed = users.reduceGroup((ratsPerUserIter, out: Collector[ ]) => determineSignature(ratsPerUserIter, out))

		// so I copied this from here: http://ci.apache.org/projects/flink/flink-docs-release-0.7/dataset_transformations.html#groupreduce-on-grouped-dataset
		val signed = users.reduceGroup {
		  (in, out: Collector[(SignatureKey, Array[Rating])]) =>
		    in foreach (x => out.collect(
				(SignatureKey(x.movie, x.stars),
						  Array(Rating(x.user, x.movie, x.stars)))
				))
    	}

		val SIGNATURE = 0
		val buckets = signed.groupBy(SIGNATURE).reduceGroup {
			(in:  Iterator[ (SignatureKey, Array[Rating]) ], out: Collector[Iterable[Rating]])  =>
				in foreach((x: (SignatureKey, Array[Rating])) => out.collect(x._2))
		}
		buckets.writeAsText("file:///tmp/flink-buckets.txt")
		//val similar = buckets.flatMap(x => compareCandidates(x))
		// val buckets: DataSet[Array[NumberOfRatingsPerUser]] = signed.groupBy(0).reduceGroup(
		// 		usersWithSameSigIterator => usersWithSameSigIterator.foldLeft(
		// 			Array[NumberOfRatingsPerUser]())((a,b) => a++b._2)) //.filter(_.size > 1)

		// resulting output is 19 times [Lde.hpi.fgis.willidennis.NumberOfRatingsPerUser;@address
		// 19 makes sense for 4 files!
		// similar.writeAsText("file:///tmp/flink-log.txt")

		env.execute

	}
}
