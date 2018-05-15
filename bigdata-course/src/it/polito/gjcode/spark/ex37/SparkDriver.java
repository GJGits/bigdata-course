package it.polito.gjcode.spark.ex37;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];
		SparkConf conf = new SparkConf().setAppName("[Spark app #37]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> pollutionRDD = context.textFile(inputPath);

		JavaPairRDD<String, Iterable<Double>> sensorValuePairRDD = pollutionRDD
				.mapToPair(
						line -> new Tuple2<String, Double>(line.split(",")[0], Double.parseDouble(line.split(",")[2])))
				.groupByKey();

		JavaPairRDD<String, Double> sensorMaxValueRDD = sensorValuePairRDD.mapToPair(tuple -> {
			double max = 0;
			for (Double value : tuple._2) {
				if (value.doubleValue() > max)
					max = value.doubleValue();
			}
			return new Tuple2<String, Double>(tuple._1, Double.valueOf(max));
		});
		
		sensorMaxValueRDD.saveAsTextFile(outputPath);
		context.close();

	}

}
