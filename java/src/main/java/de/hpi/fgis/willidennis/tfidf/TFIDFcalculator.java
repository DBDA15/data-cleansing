package de.hpi.fgis.willidennis.tfidf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
//import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class TFIDFcalculator {
	
	// from https://github.com/harryaskham/Twitter-L-LDA/blob/master/util/Stopwords.java
	public static String[] stopwords = {"a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
	

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
	        
	        
	        
	        // in how many documents does each term occur?
	        // as in https://spark.apache.org/docs/0.9.1/java-programming-guide.html
	        JavaPairRDD<String, Integer> wordOccurrences = lineItems.values().flatMap(
	        		  new FlatMapFunction<LineItem, String>() {
	        		    public Iterable<String> call(LineItem li) {
	        		      return li.wordmap.keySet();
	        		    }
	        		  } // puts all distinct words per document into one list.
	        		).mapToPair(
					  new PairFunction<String, String, Integer>() {
						    public Tuple2<String, Integer> call(String s) {
						      return new Tuple2(s, 1);
						    }
						  } // makes a pair (word, 1) out of it to prepare the reduce
						).reduceByKey(
					  new Function2<Integer, Integer, Integer>() {
						    public Integer call(Integer i1, Integer i2) {
						      return i1 + i2;
						    }
						  } // aggregates the occurrences
					  );

	        List<Tuple2<String, Integer>> wordOccsList = wordOccurrences.collect();
	        final Broadcast<List<Tuple2<String, Integer>>> bcvar = context.broadcast(wordOccsList);
	        
	        final Broadcast<Long> bctotalnumberofdocs = context.broadcast(lineItems.count());
	        
	        lineItems = lineItems.mapValues(
    				new Function<LineItem, LineItem>() {
    					public LineItem call(LineItem li) {
    						li.calcTFIDFs(bcvar, bctotalnumberofdocs);
    						return li;
    					}
    				});
	        
	        System.out.println(lineItems.take(10));
	        
	    }
	    
	    
	 }
	
	
	static class LineItem implements Serializable {
	    public String stringid;
	    public HashMap<String, Integer> wordmap;
	    public HashMap<String, Double> tfidfs;
	    public Integer wordcount;

	    public LineItem(String line) {
	      String[] values = line.split("<");
	      stringid = values[1];
	      String text = values[2].split("\"")[1];
	      
	      String[] words = text.split(" ");
	      wordcount = words.length;
	      
	      wordmap = new HashMap<String, Integer>();
	      for(String word: words) {
	    	  if(!isStopword(word)) {
	    	  	word = word.toLowerCase();
	    		if(!wordmap.containsKey(word)) {
	    			wordmap.put(word, StringUtils.countMatches(text, word));
	    		}
	    	  }
	      }
	    }

		public void calcTFIDFs(Broadcast<List<Tuple2<String, Integer>>> bcvar, Broadcast<Long> totalnumberofdocs) {
			tfidfs = new HashMap<String, Double>();
			
			// which term is most often in this doc and how often is that?
			int maxCountInThisDoc = -1;
			for(String term: wordmap.keySet()) {
				int thisCount = wordmap.get(term);
				if(thisCount>maxCountInThisDoc)  maxCountInThisDoc = thisCount;
			}
			
			for(String term: wordmap.keySet()) {
				int countThisDoc = wordmap.get(term);
				
				int countInDocs = -1;
				Iterator myiter = bcvar.getValue().iterator();
				boolean found = false;
				do{
					Tuple2<String, Integer> doccounttpl = (Tuple2<String, Integer>) myiter.next();
					if(doccounttpl._1().equals(term)) {
						countInDocs = doccounttpl._2();
						found = true;
					}
				} while (!found && myiter.hasNext());
				
				double tf = countThisDoc / maxCountInThisDoc;
				double idf = totalnumberofdocs.getValue() / countInDocs;
				double tfidf = tf/idf;
				
				tfidfs.put(term, tfidf);
			}
			
		}

		public String toString() { return stringid + ": " + tfidfs; }
	    
	  }
	
	/**
	 * src: https://github.com/harryaskham/Twitter-L-LDA/blob/master/util/Stopwords.java
	 * @param word
	 * @return
	 */
	public static boolean isStopword(String word) {
		if(word.length() < 2) return true;
		if(word.charAt(0) >= '0' && word.charAt(0) <= '9') return true; //remove numbers, "25th", etc
		if(stopWordSet.contains(word)) return true;
		else return false;
	}

}
