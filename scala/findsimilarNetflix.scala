def parseLine(line: String, movid:Int):(Int, Int, Int) = {
	val splitted = line.split(",")
	return (splitted(0).toInt, movid, splitted(1).toInt)
}

val numberOfFiles = 9
var parsed = sc.textFile("netflixdata/mv_0000001.txt").filter(!_.contains(":")).map(line => parseLine(line, 1))

for(i <- 2 to numberOfFiles) {
	val thisDataset = sc.textFile("netflixdata/mv_000000" + i + ".txt").filter(!_.contains(":")).map(line => parseLine(line, i))
	parsed = parsed ++ thisDataset 
}

/* statistics */
/* TODO: these statistics could also be generated while reading the files! */
val statistics = parsed.groupBy(_._2).map(x => (x._1, x._2.size)).sortBy(-_._2)

/* group ratings by user */
val users = parsed.groupBy(_._1) /* users: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int, Int)])] */
/* TODO: users has userID as key and then repeted in tuple. unnecessary! */


/* make signature 
*
* Broadcast
*/

def determineSignature ( n:Int, statistics:Array[Int], ratings: Iterable[(Int, Int, Int)] ) : Int = {
	var occ = 0
	for ( x <- statistics ) {
		for ( rat <- ratings )  {
			if(rat._2 == x) occ += 1; /* if that user has rated that movie */
		}
		if(occ == n) return x
    }
	return -1
}

val bcCount = sc.broadcast(statistics.map(x => x._1).collect) /* we only keep the movid, not the number of ratings */

val signed = users.map( x => (determineSignature(1, bcCount.value, x._2), Array(x)) )

/* reduce: create Array[all users with same signature]
	(key is dropped because we dont need it anymore)
*/

import Array._
val reduced = signed.reduceByKey((a,b) => concat(a,b)).values /* RDD[Array[(Int, Iterable[(Int, Int, Int)])]] */


/* flatmap: calculate similarities of all pair combinations per candidate array */
def calculateSimilarity(element: (Int, Iterable[(Int, Int, Int)]), compareTo: (Int, Iterable[(Int, Int, Int)])) : Double = {
	return 1
}

def compareCandidates(candidates: Array[(Int, Iterable[(Int, Int, Int)])]): Array[(Int,Int,Double)] = {
	var result = new Array[(Double,Int,Int)](0)
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
val similarities = reduced.flatMap(compareCandidates)
