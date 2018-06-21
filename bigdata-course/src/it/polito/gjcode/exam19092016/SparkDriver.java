package it.polito.gjcode.exam19092016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String ouputA = args[1];
		String outputB = args[2];
		double pm10Threshold = Double.parseDouble(args[3]);
		double pm25threshold = Double.parseDouble(args[4]);

		SparkConf sparkConf = new SparkConf().setAppName("spark esame 19-10-2016");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = context.textFile(inputPath);

		JavaRDD<String> highPollutedStations = inputRDD.filter(line -> line.split(",")[1].split("/")[0] == "2015")
				.mapToPair(line -> {

					String[] tokens = line.split(",");
					String stationId = tokens[0];
					double pm10 = Double.parseDouble(tokens[4]);

					return pm10 > pm10Threshold ? new Tuple2<String, Integer>(stationId, 1)
							: new Tuple2<String, Integer>(stationId, 0);

				}).reduceByKey((el1, el2) -> {
					return el1 + el2;
				}).filter(element -> element._2 > 45).map(element -> element._1);

		JavaRDD<String> alwaysPollutedStations = inputRDD.mapToPair(line -> {

			String[] tokens = line.split(",");
			String stationId = tokens[0];
			double pm25 = Double.parseDouble(tokens[5]);

			return pm25 > pm25threshold ? new Tuple2<String, Count>(stationId, new Count(1, 1))
					: new Tuple2<String, Count>(stationId, new Count(1, 1));

		}).reduceByKey((el1, el2) -> {

			int newSum = el1.getSum() + el2.getSum();
			int newCount = el1.getCount() + el2.getCount();
			return new Count(newSum, newCount);

		}).filter(element -> element._2.getSum() == element._2.getCount()).map(el -> el._1);

		highPollutedStations.saveAsTextFile(ouputA);
		alwaysPollutedStations.saveAsTextFile(outputB);

		context.close();

	}

}
