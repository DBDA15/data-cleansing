r <- read.csv("runtimes.csv", sep=";")

scaleout.flink <- r[r$framework=="f" & r$files==100 & r$sig=="1" & r$lengthclass=="n" & r$goal=="find sims",]

scaleout.spark <- r[r$framework=="s" & r$files==100 & r$sig=="1" & r$lengthclass=="n" & r$goal=="find sims",]
scaleout.spark <- scaleout.spark[c(2,3,4,5,6),]
scaleout.spark <- scaleout.spark[order(scaleout.spark$cores),]
scaleout.flink <- scaleout.flink[order(scaleout.flink$cores),]

pdf("runtime.pdf")
plot(c(scaleout.spark$cores, scaleout.flink$cores), c(scaleout.spark$time.mins, scaleout.flink$time.mins),
	main="Runtime", xlab="Workers", ylab="Runtime in minutes",
	ylim=c(0,55), type="n"
	)

lines(scaleout.spark$cores, scaleout.spark$time.mins, col="blue", pch=8, type="o")
lines(scaleout.flink$cores, scaleout.flink$time.mins, col="red", pch=19, type="o")
dev.off()

t.spark <- scaleout.spark$time.mins
t.flink <- scaleout.flink$time.mins

pdf("relative-speedup.pdf")
plot(c(scaleout.spark$cores, scaleout.flink$cores), max(t.flink)/c(t.spark, t.flink),
	main="Relative Speedup T1/Tn", xlab="Workers", ylab="Speedup",
	ylim=c(0,20), type="n",
	)

lines(scaleout.spark$cores, max(t.spark)/scaleout.spark$time.mins, col="blue", pch=8, type="o")
lines(scaleout.flink$cores, max(t.flink)/scaleout.flink$time.mins, col="red", pch=19, type="o")
abline(0,1)

dev.off()


dataset.flink <- r[r$framework=="f" & r$sig=="1" & r$lengthclass=="n" & r$goal=="find sims" & r$cores==20, ]
dataset.flink <- dataset.flink[order(dataset.flink$files), ]

pdf("scale-datasize.pdf")

plot(dataset.flink$files/10, dataset.flink$time.mins,
	main="Runtime Flink 0.9.0, 20 Workers, 4GB RAM", xlab="data set size (users) in percent", ylab="Runtime in minutes",
	ylim=c(0,145)
	)
lines(dataset.flink$files/10, dataset.flink$time.mins, col="red", pch=19)
lines(c(10, 20, 50) , c(5.1,5.1 *4, 5.1*25), lty=2, type="o")
dev.off()


