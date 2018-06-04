package it.polito.gjcode.spark.streaming.wordWindow;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.Arrays;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) throws InterruptedException {

		String outputPathPrefix;

		outputPathPrefix = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Streaming word count");

		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Set the checkpoint folder (it is needed by some window
		// transformations)
		jssc.checkpoint("checkpointfolder");

		// Create a (Receiver) DStream that will connect to localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

		// Apply the "standard" transformations to perform the word count task
		// However, the "returned" RDDs are DStream/PairDStream RDDs
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

		// Count the occurrence of each word in the current window
		JavaPairDStream<String, Integer> wordsOnes = words
				.mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));

		// reduceByKeyAndWindow is used instead of reduceByKey
		// The characteristics of the window is also specified
		JavaPairDStream<String, Integer> wordsCounts = wordsOnes.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
				Durations.seconds(30), Durations.seconds(10));

		// Print the num. of occurrences of each word of the current window
		// (only 10 of them)
		wordsCounts.print();

		// Store the output of the computation in the folders with prefix
		// outputPathPrefix
		wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");
		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
