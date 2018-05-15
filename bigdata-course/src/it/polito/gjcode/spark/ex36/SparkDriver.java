package it.polito.gjcode.spark.ex36;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		SparkConf conf = new SparkConf().setAppName("[Spark app #36]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> pollutionRDD = context.textFile(inputPath);
		JavaRDD<Double> valuesRDD = pollutionRDD.map(line -> Double.parseDouble(line.split(",")[2]));
		long numOfElements = valuesRDD.count();
		double sum = valuesRDD.reduce((e1, e2) -> e1 + e2);
		double avg = sum / numOfElements;
		System.out.println("avg: " + avg);
		context.close();
	}

}
