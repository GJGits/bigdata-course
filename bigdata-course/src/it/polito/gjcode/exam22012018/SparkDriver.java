package it.polito.gjcode.exam22012018;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String booksPath = args[0];
		String purchasePath = args[1];
		String outputA = args[2];
		String outputB = args[3];

		SparkConf conf = new SparkConf().setAppName("spark esame 22-01-2018");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Create RDDs
		JavaRDD<String> purchaseRDD = context.textFile(purchasePath).filter(x -> x.split(",")[2].startsWith("2017"));
		JavaRDD<String> booksRDD = context.textFile(booksPath);
		JavaRDD<String> booksIdRDD = booksRDD.map(x -> x.split(",")[0]);

		JavaPairRDD<String, Tuple2<Double, Double>> anomalousRDD = purchaseRDD.mapToPair(line -> {

			String[] tokens = line.split(",");
			String id = tokens[1];
			double price = Double.parseDouble(tokens[3]);
			Tuple2<Double, Double> minMax = new Tuple2<>(price, price);
			return new Tuple2<String, Tuple2<Double, Double>>(id, minMax);

		}).reduceByKey((x, y) -> {

			double xMin = x._1;
			double xMax = x._2;
			double yMin = y._1;
			double yMax = y._2;
			double max = xMax > yMax ? xMax : yMax;
			double min = xMin < yMin ? xMin : yMin;

			return new Tuple2<Double, Double>(min, max);

		}).filter(x -> (x._2._2 - x._2._1 > 15));

		List<String> notSold = booksIdRDD.subtract(purchaseRDD.map(x -> x.split(",")[1])).collect();

		JavaPairRDD<String, Double> percentageRDD = booksRDD.mapToPair(line -> {
			String[] tokens = line.split(",");
			String bid = tokens[0];
			String genre = tokens[2];
			return notSold.contains(bid) ? new Tuple2<String, Count>(genre, new Count(1, 1))
					: new Tuple2<String, Count>(genre, new Count(0, 1));
		}).reduceByKey((x, y) -> {
			int sum = x.getSum() + y.getSum();
			int count = x.getCount() + y.getCount();
			return new Count(sum, count);
		}).mapValues(count -> (double) count.getSum() / count.getCount());

		// store results
		anomalousRDD.saveAsTextFile(outputA);
		percentageRDD.saveAsTextFile(outputB);

		context.close();

	}

}
