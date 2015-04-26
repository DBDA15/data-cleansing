package de.hpi.fgis.willidennis.tfidf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
//import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import scala.Tuple2;


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
	        JavaPairRDD<String, LineItem> lineItems = context.
	        		textFile(filepath)
	        		.mapToPair(
	        				new PairFunction<String, String, LineItem>() {
	        					public Tuple2<String, LineItem> call(String line) {
	        						LineItem li = new LineItem(line);
	        						return new Tuple2<String, LineItem>(li.stringid, li);
	        					}
	        				});
	        
	        Long doccount = lineItems.count();
	        
	        System.out.println(lineItems.first());
	    }
	    
	    
	 }
	
	
	static class LineItem implements Serializable {
	    public String stringid;
	    public HashMap<String, Integer> wordmap;
	    public Integer wordcount;

	    public LineItem(String line) {
	      String[] values = line.split("<");
	      stringid = values[1];
	      String text = values[2].split("\"")[1];
	      
	      String[] words = text.split(" ");
	      wordcount = words.length;
	      
	      wordmap = new HashMap<String, Integer>();
	      for(String word: words) {
	    		if(!wordmap.containsKey(word)) {
	    			wordmap.put(word, StringUtils.countMatches(text, word));
	    		}
	      }
	    }

		public String toString() { return stringid + ": " + wordmap; }
	    
	  }

}
