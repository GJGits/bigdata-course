package it.polito.gjcode.spark.streaming.wordFolder;

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
		SparkConf conf = new SparkConf().setAppName("Spark Streaming word count");

		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Create a DStream reading the content of the input folder
		JavaDStream<String> lines = jssc.textFileStream(inputFolder);

		// Apply the "standard" transformations to perform the word count task
		// However, the "returned" RDDs are DStream/PairDStream RDDs
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

		JavaPairDStream<String, Integer> wordsOnes = words
				.mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));

		JavaPairDStream<String, Integer> wordsCounts = wordsOnes.reduceByKey((i1, i2) -> i1 + i2);

		wordsCounts.print();

		wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
