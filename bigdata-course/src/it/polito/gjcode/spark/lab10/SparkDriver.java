package it.polito.gjcode.spark.lab10;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

		// Select only tweets with hashtag
		JavaDStream<String> filteredTweets = tweets.filter(line -> line.contains("#"));

		JavaPairDStream<String, Integer> hashtagOne = filteredTweets.flatMapToPair(line -> {

			String[] tokens = line.split("\t");
			String tweet = tokens[1];
			Pattern p = Pattern.compile("#\\w+");
			Matcher m = p.matcher(tweet);
			int groupCount = m.groupCount();
			List<Tuple2<String, Integer>> hashtags = new LinkedList<>();

			for (int i = 0; i < groupCount; i++)
				hashtags.add(new Tuple2<>(m.group(i), 1));

			return hashtags.iterator();

		});

		JavaPairDStream<String, Integer> hashtagSum = hashtagOne.reduceByKeyAndWindow((v1, v2) -> {
			return v1 + v2;
		}, Durations.seconds(30), Durations.seconds(10));

		JavaPairDStream<Integer, String> orderedHashTags = hashtagSum.mapToPair(pair -> {
			return new Tuple2<Integer, String>(pair._2, pair._1);
		}).transformToPair(rdd -> rdd.sortByKey(false));

		// Print the num. of occurrences of each word of the current window
		// (only 10 of them)
		orderedHashTags.print();

		// Select only relevant hashtags
		// JavaPairDStream<String, Integer> relevantHashtags =
		// hashtagCount.filter(element -> element._2 >= 100);

		// Store the output of the computation in the folders with prefix
		// outputPathPrefix
		orderedHashTags.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
