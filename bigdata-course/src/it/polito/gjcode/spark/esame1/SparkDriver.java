package it.polito.gjcode.spark.esame1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String preferencesPath = args[0];
		String moviesPath = args[1];
		String out1Path = args[2];
		String out2Path = args[3];
		double threshold = Double.valueOf(args[4]);

		SparkConf conf = new SparkConf().setAppName("[Spark app esame1]");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Create the RDDs
		JavaRDD<String> preferencesRDD = context.textFile(preferencesPath);
		JavaRDD<String> moviesRDD = context.textFile(moviesPath);

		// Count the number of genres
		long numberOfGenres = moviesRDD.map(line -> line.split(",")[2]).distinct().count();

		// Create an RDD for useless profiles
		JavaPairRDD<String, Double> dangerousProfiles = preferencesRDD.mapToPair(line -> {

			String[] tokens = line.split(",");
			String userId = tokens[0];
			String genre = tokens[1];

			return new Tuple2<String, String>(userId, genre);

		}).groupByKey().mapToPair(element -> {

			int sum = 0;
			double percentage = 0.0;

			for (String genre : element._2)
				sum++;

			percentage = sum / numberOfGenres;

			return new Tuple2<String, Double>(element._1, percentage);
		});

		JavaRDD<String> uselessProfiles = dangerousProfiles.filter(element -> element._2 >= 0.9)
				.map(element -> element._1);

		JavaRDD<String> misleadingProfiles = dangerousProfiles.filter(element -> element._2 >= threshold)
				.map(element -> element._1);

		// Store result exercises
		uselessProfiles.saveAsTextFile(out1Path);
		misleadingProfiles.saveAsTextFile(out2Path);

		// Close context
		context.close();

	}

}
