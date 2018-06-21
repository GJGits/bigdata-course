package it.polito.gjcode.exam30062017;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String input = args[0];
		String outputA = args[1];
		String outputB = args[2];
		int nw = Integer.parseInt(args[3]);

		SparkConf conf = new SparkConf().setAppName("spark esame 30-06-2017");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> inputRDD = context.textFile(input);
		JavaRDD<String> filteredRDD = inputRDD.filter(line -> line.split(",")[1].split("/")[0] == "2016");

		JavaPairRDD<String, Double> pairBase = filteredRDD.mapToPair(line -> {
			String[] tokens = line.split(",");
			String key = tokens[0] + "," + tokens[1];
			double price = Double.parseDouble(tokens[3]);
			return new Tuple2<String, Double>(key, price);
		});

		// RDD for request a:
		JavaPairRDD<String, Double> lowerPair = pairBase.reduceByKey((el1, el2) -> el2 < el1 ? el2 : el1)
				.sortByKey(true);

		JavaPairRDD<String, Double> maxFirst = pairBase.filter(x -> Tools.firstOfTheWeek(x._1.split(",")[1]))
				.reduceByKey((x, y) -> x > y ? x : y).mapToPair(el -> {
					String date = el._1.split(",")[1];
					int week = Tools.weekOfTheYear(date);
					String key = el._1.split(",")[0] + "-" + week;
					return new Tuple2<String, Double>(key, el._2);
				});

		JavaPairRDD<String, Double> maxLast = pairBase.filter(x -> Tools.lastOfTheWeek(x._1.split(",")[1]))
				.reduceByKey((x, y) -> x > y ? x : y).mapToPair(el -> {
					String date = el._1.split(",")[1];
					int week = Tools.weekOfTheYear(date);
					String key = el._1.split(",")[0] + "-" + week;
					return new Tuple2<String, Double>(key, el._2);
				});

		// RDD for request b:
		JavaRDD<String> positiveTrends = maxFirst.join(maxLast).filter(el -> el._2._2 > el._2._1)
				.mapToPair(el -> new Tuple2<String, Integer>(el._1.split("-")[0], 1)).reduceByKey((x, y) -> x + y)
				.filter(x -> x._2 > nw).map(x -> x._1);

		lowerPair.saveAsTextFile(outputA);
		positiveTrends.saveAsTextFile(outputB);

		context.close();

	}

}
