package it.polito.gjcode.exam12072016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String stationsPath = args[0];
		String stationOccupancy = args[1];
		String output1 = args[2];
		String output2 = args[3];

		SparkConf conf = new SparkConf().setAppName("spark esame 12-07-2016");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Create RDDs
		JavaRDD<String> stationsRDD = context.textFile(stationsPath);
		JavaRDD<String> occupancyRDD = context.textFile(stationOccupancy);

		// Create an RDD for small station
		JavaRDD<String> smallStationsRDD = stationsRDD.filter(line -> Integer.parseInt(line.split(",")[3]) < 5)
				.map(line -> line.split(",")[0]);

		// Create an RDD for potential critical station
		JavaRDD<String> potentialCriticalRDD = occupancyRDD.filter(line -> Integer.parseInt(line.split(",")[4]) == 0)
				.map(line -> line.split(",")[0]).distinct();

		// Intersect to have the potential and small stations
		JavaRDD<String> smallPotentialCriticalRDD = smallStationsRDD.intersection(potentialCriticalRDD);

		// Create an RDD for well sized stations
		JavaRDD<String> wellSizedStations = occupancyRDD.mapToPair(line -> {

			String[] tokens = line.split(",");
			String stationId = tokens[0];
			int freeSlots = Integer.parseInt(tokens[3]);
			Count count = freeSlots >= 3 ? new Count(1, 1) : new Count(0, 1);
			return new Tuple2<String, Count>(stationId, count);
		}).reduceByKey((el1, el2) -> {

			int sum = el1.getSum() + el2.getSum();
			int count = el1.getCount() + el2.getCount();
			return new Count(sum, count);
		}).filter(element -> element._2.getSum() == element._2.getCount()).map(element -> element._1);

		// Save results
		smallPotentialCriticalRDD.saveAsTextFile(output1);
		wellSizedStations.saveAsTextFile(output2);

		// Close contex
		context.close();

	}

}
