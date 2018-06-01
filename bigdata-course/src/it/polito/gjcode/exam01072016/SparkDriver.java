package it.polito.gjcode.exam01072016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String boughtInput = args[0];
		String booksInput = args[1];
		double threshold = Double.parseDouble(args[2]);

		SparkConf conf = new SparkConf().setAppName("[Spark app esame 2]");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Create RDDs
		JavaRDD<String> booksRDD = context.textFile(booksInput);
		JavaRDD<String> boughtRDD = context.textFile(boughtInput);

		// Not sold RDD
		JavaRDD<String> notSoldRDD = booksRDD.subtract(boughtRDD);

		// Select only the books with a suggested price greater than 30
		JavaRDD<String> filteredBooksRDD = booksRDD.filter(line -> {

			String[] tokens = line.split(",");
			double price = Double.parseDouble(tokens[3]);
			return price > 30;
		});

	}

}
