package it.polito.gjcode.spark.ex32;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("[Spark ex #32]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> javaRDD = context.textFile(inputPath);

		String max = javaRDD.map(line -> line.split(",")[2])
				.max((String a, String b) -> Double.compare(Double.parseDouble(a), Double.parseDouble(b)));

		System.out.println("max value: " + max);

		context.close();
	}

}
