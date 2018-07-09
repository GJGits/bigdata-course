package it.polito.gjcode.exam19092017;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkRDDriver {

	public static void main(String[] args) {

		String boughtBooks = args[0];
		String bookPath = args[1];
		double minThreshold = Double.parseDouble(args[2]);
		String outA = args[3];
		String outB = args[4];

		SparkConf conf = new SparkConf().setAppName("esame 14-09-2017");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> boughtRDD = context.textFile(boughtBooks);

		// Get solution a:
		JavaRDD<String> solutionA = context.textFile(bookPath)
				.filter(line -> Double.parseDouble(line.split(",")[3]) > 30).map(line -> line.split(",")[0])
				.subtract(boughtRDD.map(line -> line.split(",")[1]));

		// Get solution b:
		JavaRDD<String> solutionB = boughtRDD.mapToPair(line -> {
			String[] tokens = line.split(",");
			String customerId = tokens[0];
			double price = Double.parseDouble(tokens[3]);
			int count = price <= 10 ? 1 : 0;
			return new Tuple2<String, Tuple2<Integer, Integer>>(customerId, new Tuple2<>(count, 1));
		}).reduceByKey((el1, el2) -> {
			int newSum = el1._1 + el2._1;
			int newCount = el1._2 + el2._2;
			return new Tuple2<Integer, Integer>(newSum, newCount);
		}).filter(el -> (double) (el._2._1 / el._2._2) >= minThreshold).map(el -> el._1);

		solutionA.saveAsTextFile(outA);
		solutionB.saveAsTextFile(outB);

		context.close();

	}

}
