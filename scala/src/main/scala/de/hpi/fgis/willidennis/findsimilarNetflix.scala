package de.hpi.fgis.willidennis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import Array._

object Main extends App {

	/*
	* 
	*/
	def determineSignature (user: (Int, Iterable[(Int, Int, Int)]) ) : Array[(Int, Array[ Iterable[(Int, Int, Int)] ] )] = {
		val ratings = user._2
		val result = new Array[(Int, Array[ Iterable[(Int, Int, Int)] ] )] (ratings.size)
		var i = 0
		for ( rat <- ratings )  {
			result(i) = (rat._2, Array(ratings))
			i = i+1
		}
		return result
	}

	def calculateSimilarity(user1: Iterable[(Int, Int, Int)], user2: Iterable[(Int, Int, Int)]) : Double = {
		val u1set = user1.map(x => (x._2, x._3)).toSet
		val u2set = user2.map(x => (x._2, x._3)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	def compareCandidates(candidates: Array[ Iterable[(Int, Int, Int)] ]): Array[(Int,Int,Double)] = {		
		val SIMTHRESHOLD = 0.9 /* TODO: where else can we set this!? */


		var result = new Array[(Int,Int,Double)](500*500)
		var arrayIndex = 0

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
				
				if(sizesInRange) {
					val simvalue = calculateSimilarity(user1, user2)
					if(simvalue >= SIMTHRESHOLD) {
						result(arrayIndex) = (user1.head._1, user2.head._1, simvalue)
						arrayIndex = arrayIndex +1
					}
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
		var i = 1
		var parsed = sc.textFile(TRAINING_PATH+"mv_" + "%07d".format(i) + ".txt").filter(!_.contains(":")).map(line => parseLine(line, 1))

		val fileRDDs = new Array[org.apache.spark.rdd.RDD[(Int, Int, Int)]](numberOfFiles/200)

		/* split RDD every N files to avoid stackoverflow */
		val splitRDDeveryNfiles = 200
		for(i <- 2 to numberOfFiles) {
			val thisDataset = sc.textFile(TRAINING_PATH+"mv_" + "%07d".format(i) + ".txt").filter(!_.contains(":")).map(line => parseLine(line, i))
			if(i%splitRDDeveryNfiles==0 && i>0) {
				fileRDDs((i/splitRDDeveryNfiles)-1) = parsed
				parsed = thisDataset
			}
			else {
				parsed = parsed ++ thisDataset
			}	
		}
		
		/* concat all temporary rdds */
		for(myrdd <- fileRDDs) {
			parsed = parsed ++ myrdd
		}

		/* group ratings by user */
		val users = parsed.groupBy(_._1) /* users: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int, Int)])] */
		/* TODO: users has userID as key and then repeated in tuple. unnecessary! */


		/* make signature
		* 	yields RDD[(signature, user)].
		*	user represented by iterable of all his ratings.
		*	formally RDD[(String, Array[Iterable[(Int, Int, Int)])])
		*/
		val signed = users.flatMap(determineSignature)

		/* reduce
		*/

		val reduced = signed.reduceByKey((a,b) => concat(a,b)).values.filter(_.size > 1)

		val similarities = reduced.flatMap(compareCandidates).filter(_ != null)
		//reduced.map(x => (x.size)).saveAsTextFile(RESULTS_PATH)
		//similarities.saveAsTextFile(RESULTS_PATH)
		
		println(s"\n\n ####### Similar pairs: ${similarities.count()} ###### \n\n")
		println(s"\n\n ####### Ratings: ${parsed.count()} ###### \n\n")
		println(s"\n\n ####### Users-Signatures: ${signed.count()} ###### \n\n")	
	}
}
