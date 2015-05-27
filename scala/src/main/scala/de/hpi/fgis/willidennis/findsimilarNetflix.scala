package de.hpi.fgis.willidennis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import Array._

object Main extends App {

	val simThreshold = 0.5

	def determineSignature ( n:Int, statistics:Array[Int], ratings: Iterable[(Int, Int, Int)] ) : String = {
		var result = ""
		var occ = 0
		for ( x <- statistics ) {
			for ( rat <- ratings )  {
				if(rat._2 == x) {
					result += "," + x
					occ += 1
				} /* if that user has rated that movie */
			}
			if(occ == n) return result
	    }
		return null
	}

	/* flatmap: calculate similarities of all pair combinations per candidate array */
	def calculateSimilarity(user1: (Int, Iterable[(Int, Int, Int)]), user2: (Int, Iterable[(Int, Int, Int)])) : Double = {
		val u1set = user1._2.map(x => (x._2, x._3)).toSet
		val u2set = user2._2.map(x => (x._2, x._3)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	def compareCandidates(candidates: Array[(Int, Iterable[(Int, Int, Int)])]): Array[(Int,Int,Double)] = {
		var result = new Array[(Int,Int,Double)](0)
		for(i<-0 to (candidates.length-2)) {
			var element = candidates(i)
			/* compare with all elements that follow */
			for(n<-(i+1) to (candidates.length-1)) {
				var compareTo = candidates(n)

				/* calculate similarity and add to result */
				val simvalue = calculateSimilarity(element, compareTo)
				if(simvalue > simThreshold)	{
					result = concat(result, Array((element._1, compareTo._1, simvalue)))
				}
			}
		}
		return result
	}

	def parseLine(line: String, movid:Int):(Int, Int, Int) = {
		val splitted = line.split(",")
		return (splitted(0).toInt, movid, splitted(1).toInt) // (userid, movid, rating)
	}


	override def main(args: Array[String]) = {
		var numberOfFiles = 4
		var numberOfMoviesForSig = 2
		var TRAINING_PATH = "netflixdata/training_set/"
		var RESULTS_PATH = "result"

		if(args.size > 0) {
			TRAINING_PATH = args(0)
		}
		if(args.size > 1) {
			numberOfFiles = args(1).toInt
		}

		if(args.size > 2) {
			numberOfMoviesForSig = args(2).toInt
		}

		if(args.size > 3) {
			RESULTS_PATH = args(3)
		}		

		var conf = new SparkConf()
		conf.setAppName(Main.getClass.getName)
		conf.set("spark.executor.memory", "4g")
		val sc = new SparkContext(conf)

		// build empty RDD, not that empty though
		var parsed = sc.textFile(TRAINING_PATH+"mv_0000001.txt").filter(!_.contains(":")).map(line => parseLine(line, 1))

		for(i <- 2 to numberOfFiles) {
			val thisDataset = sc.textFile(TRAINING_PATH+"mv_" + "%07d".format(i) + ".txt").filter(!_.contains(":")).map(line => parseLine(line, i))
			parsed = parsed ++ thisDataset
		}

		/* statistics */
		/* TODO: these statistics could also be generated while reading the files! */
		val statistics = parsed.groupBy(_._2).map(x => (x._1, x._2.size)).sortBy(_._2)

		/* group ratings by user */
		val users = parsed.groupBy(_._1) /* users: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int, Int)])] */
		/* TODO: users has userID as key and then repeted in tuple. unnecessary! */


		/* make signature
		*
		* Broadcast
		* N: how many movie ids are used for the sig
		*/

		val bcCount = sc.broadcast(statistics.map(x => x._1).collect) /* we only keep the movid, not the number of ratings */


		/* 	yields RDD[(signature, user)].
		*	user represented by iterable of all his ratings.
		*	formally RDD[(String, Array[(Int, Iterable[(Int, Int, Int)])])]
		*/
		val signed = users.map( x => (determineSignature(numberOfMoviesForSig, bcCount.value, x._2), Array(x)) ).filter(_._1 != null)

		/* reduce: create Array[all users with same signature]
			(key is dropped because we dont need it anymore)
			yields RDD[Array[Array[(Int, Iterable[(Int, Int, Int)])]]
			interpreted as follows:
				inner Array is one user represented by all his votings
				outer array is a bucket of all users with the same signature. (we dropped the sig string bfore)
				RDD is the list of buckets by signature
			reduced.count : how many movie(-combinations) have produced a bucket of candidates?
			reduced.map(x => (x.size)).collect: How big are the buckets?
				Important to anticipate runtime of compareCandidates!
		*/

		val reduced = signed.reduceByKey((a,b) => concat(a,b)).values /* RDD[Array[(Int, Iterable[(Int, Int, Int)])] */

		val similarities = reduced.flatMap(compareCandidates)
		similarities.saveAsTextFile(RESULTS_PATH)

		println(s"\n\n ####### Ratings: ${parsed.collect().size} ###### \n\n")
		println(s"\n\n ####### Users: ${signed.collect().size} ###### \n\n")
		println(s"\n\n ####### Similar pairs: ${similarities.collect().size} ###### \n\n")
	}
}
