package it.polito.gjcode.spark.ex33;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		SparkConf conf = new SparkConf().setAppName("[Spark app #33]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> pollutionRDD = context.textFile(inputPath);
		List<String> top3 = new ArrayList<>(pollutionRDD.map(line -> line.split(",")[2]).top(3));
        System.out.println(top3);
        context.close();
	}

}
