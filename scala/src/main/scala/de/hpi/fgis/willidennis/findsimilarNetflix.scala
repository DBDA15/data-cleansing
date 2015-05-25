
package de.hpi.fgis.willidennis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import Array._


object Main extends App {
	val numberOfFiles = 4
	val numberOfMoviesForSig = 2

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
	def calculateSimilarity(element: (Int, Iterable[(Int, Int, Int)]), compareTo: (Int, Iterable[(Int, Int, Int)])) : Double = {
		// WM TODO: calculate % of movies in common
		return 1
	}

	def compareCandidates(candidates: Array[(Int, Iterable[(Int, Int, Int)])]): Array[(Int,Int,Double)] = {
		var result = new Array[(Int,Int,Double)](0)
		for(i<-0 to (candidates.length-2)) {
			var element = candidates(i)
			/* compare with all elements that follow */
			for(n<-(i+1) to (candidates.length-1)) {
				var compareTo = candidates(n)
				/* calculate similarity and add to result */
				result = concat(result, Array((element._1, compareTo._1, calculateSimilarity(element, compareTo))))
			}
		}
		return result
	}

	def parseLine(line: String, movid:Int):(Int, Int, Int) = {
		val splitted = line.split(",")
		return (splitted(0).toInt, movid, splitted(1).toInt)
	}

	val conf = new SparkConf()
	conf.setAppName(Main.getClass.getName)
	conf.set("spark.hadoop.validateOutputSpecs", "false");
	conf.set("spark.executor.memory", "4g");
	// on server: --conf TRAINING_PATH="hdfs://tenemhead2/data/data-cleansing/netflixdata"
	val TRAINING_PATH = conf.get("TRAINING_PATH", "netflixdata/training_set/")
	val sc = new SparkContext(conf)

	if (conf.get("LogLevel") == "Off") {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
	}

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
	println(similarities.take(5))
	similarities.saveAsTextFile("result")
}