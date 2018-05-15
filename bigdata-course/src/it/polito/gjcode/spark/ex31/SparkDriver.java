package it.polito.gjcode.spark.ex31;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("[Spark ex #31]");

		// Create a Spark Context object
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> logRDD = context.textFile(inputPath);
		JavaRDD<String> filteredIPRDD = logRDD.filter(line -> line.toLowerCase().contains("www.google.com"))
				.map(line -> line.split(" - - ")[0].trim()).distinct();
		
		filteredIPRDD.saveAsTextFile(outputPath);
		
		context.close();

	}

}
