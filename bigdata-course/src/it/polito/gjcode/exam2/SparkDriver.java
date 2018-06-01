package it.polito.gjcode.exam2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath1 = args[1];
		String outputPath2 = args[2];
		double threshold = Double.parseDouble(args[3]);

		SparkConf conf = new SparkConf().setAppName("[Spark app esame 2]");
		JavaSparkContext context = new JavaSparkContext(conf);

		// RDD for the first exercise

		JavaPairRDD<String, Integer> criticalReadingRDD = context.textFile(inputPath).filter(line -> {

			String[] tokens = line.split(",");
			double pm10Reading = Double.parseDouble(tokens[2]);
			return pm10Reading > threshold;

		}).mapToPair(line -> {

			String[] lineTokens = line.split(",");
			String[] timeTokens = lineTokens[1].split("-");
			String key = String.join("-", timeTokens[0], timeTokens[2]);
			return new Tuple2<String, Integer>(key, 1);

		}).reduceByKey((val1, val2) -> {
			return val1 + val2;
		});

		JavaRDD<String> criticalPeriods = context.textFile(inputPath).filter(line -> {

			String[] tokens = line.split(",");
			double pm10Reading = Double.parseDouble(tokens[2]);
			return pm10Reading > threshold;

		}).mapToPair(line -> {

			String[] tokens = line.split(",");
			String sensorId = tokens[0];
			String date = tokens[1];

			return new Tuple2<String, String>(sensorId, date);

		}).groupByKey().flatMap(element -> {
			return Tools.getCriticalPeriods(element._2);
		});
		
		criticalReadingRDD.saveAsTextFile(outputPath1);
		criticalPeriods.saveAsTextFile(outputPath2);
		
		context.close();

	}

}
