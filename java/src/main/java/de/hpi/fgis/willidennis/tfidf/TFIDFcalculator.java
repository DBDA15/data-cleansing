package de.hpi.fgis.willidennis.tfidf;

import java.io.Serializable;
//import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;


public class TFIDFcalculator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filepath = args[0];
		
		// initialize spark environment
	    SparkConf config = new SparkConf().setAppName(TFIDFcalculator.class.getName());
	    config.set("spark.hadoop.validateOutputSpecs", "false");
	    try(JavaSparkContext context = new JavaSparkContext(config)) {
	    	// load lineitems
	        JavaRDD<String> lines = context.textFile(filepath);
	        System.out.println(lines.count());
	    	}
	 }
	

}
