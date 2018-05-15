package it.polito.gjcode.spark.ex30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("[Spark ex #30]");

		// Create a Spark Context object
		JavaSparkContext context = new JavaSparkContext(conf);

		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the
		// input file
		JavaRDD<String> logRDD = context.textFile(inputPath);

		// Solution based on an named class
		// An object of the FilterGoogle is used to filter the content of the
		// RDD.
		// Only the elements of the RDD satisfying the filter imposed by means
		// of the call method of the FilterGoogle class are included in the
		// googleRDD RDD
		JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));

		// Store the result in the output folder
		googleRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		context.close();

	}

}
