package it.polito.gjcode.spark.lab10;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.Arrays;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) throws InterruptedException {

		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Streaming Lab 10");

		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");

		JavaDStream<String> tweets = jssc.textFileStream(inputFolder);

		// Apply the "standard" transformations to perform the word count task
		// However, the "returned" RDDs are DStream/PairDStream RDDs
		JavaDStream<String> hashtags = tweets.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
				.filter(line -> line.startsWith("#"));

		// Count the occurences of hashtags and sort by value
		JavaPairDStream<String, Integer> hashtagCount = hashtags.mapToPair(line -> new Tuple2<String, Integer>(line, 1))
				.reduceByKeyAndWindow((el1, el2) -> el1 + el2, Durations.seconds(30), Durations.seconds(10))
				.transformToPair(s -> s.sortByKey(false));

		// Print the num. of occurrences of each word of the current window
		// (only 10 of them)
		hashtagCount.print();

		// Select only relevant hashtags
		// JavaPairDStream<String, Integer> relevantHashtags =
		// hashtagCount.filter(element -> element._2 >= 100);

		// Store the output of the computation in the folders with prefix
		// outputPathPrefix
		hashtagCount.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
