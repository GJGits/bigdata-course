package it.polito.gjcode.spark.lab6;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #6");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		// transpose the RDD into a pairRDD
		// output contains <userid> -> <list of products>
		JavaPairRDD<String, Iterable<String>> userProdsRDD = inputRDD.mapToPair(line -> {
			String[] lineTokens = line.split(",");
			return new Tuple2<String, String>(lineTokens[2], lineTokens[1]);
		}).groupByKey();

		JavaPairRDD<String, Iterable<Integer>> prodOnes = userProdsRDD.values().flatMapToPair(element -> {
			List<Tuple2<String, Integer>> list = new ArrayList<>();
			for (String prod1 : element) {
				for (String prod2 : element) {
					String toEmit = prod1.compareTo(prod2) < 0 ? prod1 + prod2 : prod2 + prod1;
					list.add(new Tuple2<String, Integer>(toEmit, Integer.valueOf(1)));
				}
			}
			return list.iterator();
		}).groupByKey();

		Comparator<Tuple2<String, Integer>> comp = (Tuple2<String, Integer> tup1,
				Tuple2<String, Integer> tup2) -> tup1._2 - tup2._2;

		List<Tuple2<String, Integer>> pairVal = prodOnes.mapToPair(element -> {
			int val = 0;
			for (Integer one : element._2) {
				val += one.intValue();
			}
			return new Tuple2<String, Integer>(element._1, Integer.valueOf(val));
		}).top(10, comp);

		JavaRDD<Tuple2<String, Integer>> resultRDD = sc.parallelize(pairVal);

		// Store the result in the output folder
		resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}

}
