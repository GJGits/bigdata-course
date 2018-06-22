package it.polito.gjcode.exam14072017;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputA = args[1];
		String outputB = args[2];

		SparkConf conf = new SparkConf().setAppName("spark exam 14-07-2017");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> inputRDD = context.textFile(inputPath).filter(line -> {
			String date = line.split(",")[0].replaceAll("/", "");
			return date.compareTo("20150601") >= 0 && date.compareTo("20150831") <= 0;
		});

		JavaPairRDD<String, Double> avgCityCountry = inputRDD.mapToPair(line -> {
			String[] tokens = line.split(",");
			String city = tokens[1];
			String country = tokens[2];
			String key = city + "-" + country;
			double value = Double.parseDouble(tokens[3]);
			return new Tuple2<String, Count>(key, new Count(value, 1));
		}).reduceByKey((x, y) -> new Count((x.getSum() + y.getSum()), x.getCount() + y.getCount()))
				.mapValues(el -> el.getSum() / el.getCount());

		JavaPairRDD<String, Double> avgCountry = inputRDD.mapToPair(line -> {
			String[] tokens = line.split(",");
			String country = tokens[2];
			String key = country;
			double value = Double.parseDouble(tokens[3]);
			return new Tuple2<String, Count>(key, new Count(value, 1));
		}).reduceByKey((x, y) -> new Count((x.getSum() + y.getSum()), x.getCount() + y.getCount()))
				.mapValues(el -> el.getSum() / el.getCount());

		Map<String, Double> countriesAvg = avgCountry.collectAsMap();

		JavaRDD<String> hotCities = avgCityCountry.filter(element -> {
			double avg = element._2 + 5;
			double countryAvg = countriesAvg.get(element._1.split("-")[1]);
			return avg > countryAvg;
		}).map(el -> el._1.split("-")[0]);

		avgCityCountry.saveAsTextFile(outputA);
		hotCities.saveAsTextFile(outputB);

		context.close();

	}

}
