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
val users = parsed.groupBy(_._1) /* TODO: userID as key and then repeted in tuple. unnecessary! */

