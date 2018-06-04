package it.polito.gjcode.spark.streaming.wordStateful;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkDriver {

	@SuppressWarnings("resource")
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

		JavaPairDStream<String, Integer> wordsOnes = words
				.mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));

		// DStream made of get cumulative counts that get updated in every batch
		JavaPairDStream<String, Integer> totalWordsCounts = wordsOnes.updateStateByKey(
				(Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (List<Integer> newValues,
						Optional<Integer> state) -> {
					// state.or(0) returns the value of State
					// or the default value 0 if state is not defined
					Integer newSum = state.or(0);

					// Iterates over the new values and sum them to the previous
					// state value
					for (Integer value : newValues) {
						newSum += value;
					}

					return Optional.of(newSum);
				});

		totalWordsCounts.print();

		totalWordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();

		jssc.awaitTerminationOrTimeout(120000);

		jssc.close();

	}
}
