package de.hpi.fgis.willidennis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.ArrayBuffer
import Array._

case class Rating(user:Int, movie:Int, stars:Int)
case class SignatureKey(movie:Int, stars:Int)

object Main extends App {

	def determineSignature (user: (Int, Iterable[Rating]), SIGNATURE_SIZE:Int ) : Array[(String, Array[Iterable[Rating]])] = {
		val SIMTHRESHOLD = 0.9
		val ratings = user._2

		val signatureLength = ratings.size - math.ceil(SIMTHRESHOLD*ratings.size).toInt + SIGNATURE_SIZE

		//val ratingsWithSignatures = new Array[(SignatureKey, Array[Iterable[Rating]])](signatureLength)
		val sortedRatings = ratings.toArray.sortBy(_.movie)
		val prefix = sortedRatings.slice(0, signatureLength).toList
		val signatures = combinations(prefix, SIGNATURE_SIZE).toArray
		val ratingsWithSignatures = new Array[(String, Array[Iterable[Rating]])] (signatures.length)
		for(i <- 0 to signatures.length - 1) {
			val longSignature = signatures(i).map((s:Rating) => SignatureKey(s.movie, s.stars))
			val signatureString = longSignature.map(x => x.movie.toString + ',' +x.stars.toString).mkString(";")
			ratingsWithSignatures(i) = ( (signatureString, Array(ratings)) )
		}
		return ratingsWithSignatures
	}

	def combinations[T](aList:List[T], n:Int) : Iterator[List[T]] = {
		if(aList.length < n) return Iterator(aList) // aList.combinations(n) would be an empty List
		return aList.combinations(n)
	}

	def calculateSimilarity(user1: Iterable[Rating], user2: Iterable[Rating]) : Double = {
		val u1set = user1.map(x => (x.movie, x.stars)).toSet
		val u2set = user2.map(x => (x.movie, x.stars)).toSet

		return u1set.intersect(u2set).size.toDouble / u1set.union(u2set).size.toDouble
	}

	def compareCandidates(candidates: Array[ Iterable[Rating] ]): ArrayBuffer[(Int, Int)] = {
		val SIMTHRESHOLD = 0.9
		var numberOfSims = 0.toLong
		var comparisonsRaw = 0.toLong
		var comparisonsEffective = 0.toLong

		val result = ArrayBuffer[(Int, Int)]()

		for(i<-0 to (candidates.length-2)) {
			var user1 = candidates(i)

			/* compare with all elements that follow */
			for(n<-(i+1) to (candidates.length-1)) {
				var user2 = candidates(n)

				/* calculate similarity and add to result if sizes are close enough (depends on SIMTHRESHOLD) */
				comparisonsRaw += 1
				if(lengthFilter(user1.size, user2.size, SIMTHRESHOLD)) {
					val simvalue = calculateSimilarity(user1, user2)
					comparisonsEffective += 1
					if(simvalue >= SIMTHRESHOLD) {
						numberOfSims += 1
						result.append((math.min(user1.head.user, user2.head.user), math.max(user1.head.user, user2.head.user)))
					}
				}
			}
		}
		return result
	}

	def lengthFilter(size1: Int, size2: Int, threshold:Double): Boolean = {
		return math.max(size1, size2)*threshold <= math.min(size1, size2)
	}

	def parseLine(line: String, movid:Int): Rating = {
		val splitted = line.split(",")
		return Rating(splitted(0).toInt, movid, splitted(1).toInt) // (userid, movid, rating)
	}

	def parseFiles(sc:SparkContext, TRAINING_PATH: String, numberOfFiles: Int, firstNLineOfFile: Int) : RDD[Rating] = {
		var parsed = sc.parallelize(Array[Rating]())	// build empty RDD

		val fileRDDs = new Array[org.apache.spark.rdd.RDD[Rating]](numberOfFiles/200)

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
		return parsed
	}

	override def main(args: Array[String]) = {
		val timeAtBeginning = System.currentTimeMillis

		var firstNLineOfFile = -1
		var numberOfFiles = 4
		var numberOfMoviesForSig = 2
		var TRAINING_PATH = "netflixdata/training_set/"
		var SIGNATURE_SIZE = 1
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
			SIGNATURE_SIZE = args(3).toInt
		}

		if(args.size > 4) {
			NROFCORES = args(4).toInt
		}

		var conf = new SparkConf()
		conf.setAppName(Main.getClass.getName)
		conf.set("spark.executor.memory", "4g")
		val sc = new SparkContext(conf)

		val ratings = parseFiles(sc, TRAINING_PATH, numberOfFiles, firstNLineOfFile).cache()
		val users = ratings.groupBy(_.user)

		val signed = users.flatMap(x => determineSignature(x, SIGNATURE_SIZE))
		val buckets = signed.reduceByKey((a,b) => a ++ b).values.filter(_.size > 1)
		val similarities = buckets.flatMap(compareCandidates).cache()
		val simcount = similarities.count
		println(s"\n ####### Similarities before duplicate removal: ${simcount} ###### \n\n")

		val noduplicates = similarities.distinct()
		val nodupcount = noduplicates.count
		println(s"\n ####### Similarities after duplicate removal: ${nodupcount} ###### \n\n")
		println(s"\n ####### Duplicates: ${1-(nodupcount/simcount)}%###### \n\n")

		//reduced.map(x => (x.size)).saveAsTextFile(RESULTS_PATH)
		//calcStatistics.saveAsTextFile(RESULTS_PATH)

		println(s"\n\n ####### Ratings: ${ratings.count} in ${numberOfFiles} files (first ${firstNLineOfFile} lines), ${(System.currentTimeMillis-timeAtBeginning)/1000}s ${NROFCORES} cores ###### \n")
		//println(s"\n ####### Users-Signatures: ${signed.count()} ###### \n\n")
		//println(s"\n ####### Statistics: ${statistics(2)} | ${statistics(1)} | ${statistics(0)} ###### \n")
		//println(s"\n ####### Statistics: ${statistics(2)} | ${statistics(1)} | ${statistics(0)} ###### \n")
	}
}
