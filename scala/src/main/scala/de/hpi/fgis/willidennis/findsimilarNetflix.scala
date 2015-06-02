package de.hpi.fgis.willidennis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import Array._

object Main extends App {

	def determineSignature (user: (Int, Iterable[(Int, Int, Int)]) ) : Array[((Int,Int) ,Array[Int] )] = {
		val ratings = user._2
		val result = new Array[((Int,Int), Array[Int] )] (ratings.size)
		var i = 0
		for ( rat <- ratings )  {
			result(i) = ((rat._2, rat._3), Array(user._1))
			i = i+1
		}
		return result
	}

	def calculateSimilarity(user1: Iterable[(Int, Int, Int)], user2: Iterable[(Int, Int, Int)]) : Double = {
		val u1set = user1.map(x => (x._2, x._3)).toSet
		val u2set = user2.map(x => (x._2, x._3)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	/*
	*	out: (Int, Int) = (number of similarities found, number of comparisons, number of comparisons saved)
	*/
	def generateCandidates(candidates: Array[Int] ): Array[(Int,Int)] = {
		val candLength = candidates.length
		val result = new Array[(Int, Int)]((0.5*(candLength-1)*(candLength)).toInt) // Gauß'sche Summenformel based on candLength-1
		var index = 0
		for(i<-0 to (candLength-2)) {
			for(n<-(i+1) to (candLength-1)) {
				result(index) = (candidates(i), candidates(n))
				index += 1
			}			
		}
		return result
	}

	def parseLine(line: String, movid:Int):(Int, Int, Int) = {
		val splitted = line.split(",")
		return (splitted(0).toInt, movid, splitted(1).toInt) // (userid, movid, rating)
	}


	override def main(args: Array[String]) = {
		val timeAtBeginning = System.currentTimeMillis
		val SIMTHRESHOLD = 0.9

		var firstNLineOfFile = -1
		var numberOfFiles = 4
		var numberOfMoviesForSig = 2
		var TRAINING_PATH = "netflixdata/training_set/"
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

		var conf = new SparkConf()
		conf.setAppName(Main.getClass.getName)
		conf.set("spark.executor.memory", "4g")
		val sc = new SparkContext(conf)

		/* File input
		*
		*/
		var parsed = sc.parallelize(Array[(Int, Int, Int)]())	// build empty RDD

		val fileRDDs = new Array[org.apache.spark.rdd.RDD[(Int, Int, Int)]](numberOfFiles/200)

		/* split RDD every N files to avoid stackoverflow */
		val splitRDDeveryNfiles = 200
		for(i <- 1 to numberOfFiles) {
			var thisDataset = sc.textFile(TRAINING_PATH+"mv_" + "%07d".format(i) + ".txt").filter(!_.contains(":")).map(line => parseLine(line, i))
			
			if(firstNLineOfFile> (-1)) {
				thisDataset = sc.parallelize(thisDataset.take(firstNLineOfFile))
			}

			if(i%splitRDDeveryNfiles==0) {
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
		val fullusers = parsed.groupBy(_._1) /* fullusers: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int, Int)])] */
		/* TODO: users has userID as key and then repeated in tuple. unnecessary! */


		/* make signature
		* 	yields RDD[(signature, user)].
		*	user represented by only his ID!.
		*/
		val signed = fullusers.flatMap(determineSignature)

		/* reduce
		*/

		val buckets = signed.reduceByKey((a,b) => concat(a,b)).values.filter(_.size > 1)

		/* self-join (or cartesian?) for candidate generation
		* with rdd I could do myrdd.cartesian(myrdd).filter(a => a._1 > a._2)
		*		List of RDDs (one rdd for each bucket) would be a solution
		* but I have an array
		* thus run flatmap using custom method <candidateGeneration> => RDD[(int, int)]
		* TODO: Length filter could be applied in cGen method!
		*/

		val candidatePairs = buckets.flatMap(generateCandidates)

		// TODO: remove dups that happen due to occurrences in multiple buckets

		// join data
		// use first id to join one. than map to get other key. than join second
		val join1 = candidatePairs.join(fullusers)
		val join2prep = join1.map(a => (a._2._1, (a._1, a._2._2) ) )
		val join2 = join2prep.join(fullusers)
		// yields: RDD[(Int, ((Int, Iterable), Iterable))]
		
		// do comparison
		val result = join2.map(a => (a._1, a._2._1._1, calculateSimilarity(a._2._1._2, a._2._2))).filter(_._3 > SIMTHRESHOLD)
		//result.saveAsTextFile(RESULTS_PATH)
				
		println(s"\n\n ####### Ratings: ${parsed.count()} in ${numberOfFiles} files (first ${firstNLineOfFile} lines), ${(System.currentTimeMillis-timeAtBeginning)/1000}s ${NROFCORES} cores ###### \n")
		println(s"\n ####### Similarities: ${result.count()} ###### \n")
	}
}
