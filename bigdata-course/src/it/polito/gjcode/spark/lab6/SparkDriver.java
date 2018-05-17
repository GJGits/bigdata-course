package it.polito.gjcode.spark.lab6;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab6");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> reviewsRDD = sc.textFile(inputPath);

		JavaRDD<String> reviewsRDDnoHeader = reviewsRDD.filter(line -> !line.startsWith("Id"));

		// Generate one pair UserId, ProductId for each input line
		JavaPairRDD<String, String> pairUserProduct = reviewsRDDnoHeader.mapToPair(s -> {
			String[] features = s.split(",");

			return new Tuple2<String, String>(features[2], features[1]);
		});

		// Generate one "transaction" for each user
		// <user_id> → < list of the product_ids reviewed>
		JavaPairRDD<String, Iterable<String>> UserIDListOfReviewedProducts = pairUserProduct.groupByKey();

		// We are interested only in the value part (the lists of products that have
		// been reviewed together)

		JavaRDD<Iterable<String>> transactions = UserIDListOfReviewedProducts.values();

		// Generate a PairRDD of (key,value) pairs. One pair for each combination of
		// products
		// appearing in the same transaction
		// - key = pair of products reviewed together
		// - value = 1
		JavaPairRDD<String, Integer> pairsOfProductsOne = transactions.flatMapToPair(products -> {
			List<Tuple2<String, Integer>> results = new ArrayList<>();

			for (String p1 : products) {
				for (String p2 : products) {
					if (p1.compareTo(p2) > 0)
						results.add(new Tuple2<String, Integer>(p1 + " " + p2, 1));
				}
			}

			return results.iterator();
		});

		// Count the frequency (i.e., number of occurrences) of each key (= pair of
		// products)
		JavaPairRDD<String, Integer> pairsFrequencies = pairsOfProductsOne
				.reduceByKey((count1, count2) -> count1 + count2);

		// Select only the pairs that appear more than once and their frequencies.
		JavaPairRDD<String, Integer> atLeast2PairsFrequencies = pairsFrequencies.filter(t -> t._2() > 1);

		// Take the first 10 elements of pairsFrequencies
		// based on top + a personalized comparator
		List<Tuple2<String, Integer>> topList = atLeast2PairsFrequencies.top(10, new FreqComparatorTop());

		for (Tuple2<String, Integer> productFrequency : topList) {
			System.out.println(productFrequency);
		}

		// Close the Spark context
		sc.close();
	}

}
