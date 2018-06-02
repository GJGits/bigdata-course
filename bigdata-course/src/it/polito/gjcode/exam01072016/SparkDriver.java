package it.polito.gjcode.exam01072016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String boughtInput = args[0];
		String booksInput = args[1];
		String output1 = args[3];
		String output2 = args[4];
		double threshold = Double.parseDouble(args[2]);

		SparkConf conf = new SparkConf().setAppName("[Spark app esame 2]");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Create RDDs
		JavaRDD<String> booksRDD = context.textFile(booksInput);
		JavaRDD<String> boughtRDD = context.textFile(boughtInput);

		// Select only the books with a suggested price greater than 30
		JavaRDD<String> expensiveBooksRDD = booksRDD.filter(line -> {

			String[] tokens = line.split(",");
			double price = Double.parseDouble(tokens[3]);
			return price > 30;
		});

		// Expensive not sold RDD
		JavaRDD<String> notSoldRDD = expensiveBooksRDD.subtract(boughtRDD).map(line -> line.split(",")[0]);
		long countOfNotSold = notSoldRDD.count();

		System.out.println("NUMBER OF EXPENSIVE AND NOT SOLD BOOKS: " + countOfNotSold);

		// final RDD for b request of 2nd exercise
		JavaRDD<String> cheapPurchaseRDD = boughtRDD.mapToPair(line -> {

			String[] tokens = line.split(",");
			String customerRDD = tokens[0];
			double purchaseValue = Double.parseDouble(tokens[3]);
			Tuple2<String, Count> count = purchaseValue < 10 ? new Tuple2<>(customerRDD, new Count(1, 1))
					: new Tuple2<>(customerRDD, new Count(0, 1));

			return count;

		}).reduceByKey((el1, el2) -> {

			int sum = el1.getSum() + el2.getSum();
			int count = el1.getCount() + el2.getCount();

			return new Count(sum, count);

		}).mapToPair(element -> {

			double avg = (double) ((double) element._2.getSum() / (double) element._2.getCount());
			return new Tuple2<String, Double>(element._1, avg);

		}).filter(element -> element._2 > threshold).map(element -> element._1);

		cheapPurchaseRDD.saveAsTextFile(output2);
		notSoldRDD.saveAsTextFile(output1);
		context.close();

	}

}
