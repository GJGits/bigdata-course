package it.polito.gjcode.spark.lab7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		// Arguments
		String registerPath = args[0];
		String stationsPath = args[1];
		double criticalityTrashold = Double.parseDouble(args[2]);
		String outputNameFileKML = args[3];

		// Create context and RDDs

		SparkConf conf = new SparkConf().setAppName("[Spark app lab7]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> registersRDD = context.textFile(registerPath);

		// Delete header from stations.csv file and create station pair RDD.

		JavaPairRDD<String, String> stationsRDD = context.textFile(stationsPath)
				.filter(line -> line.split("\\s+")[0] != "stationId").mapToPair(line -> {
					String[] tokensLine = line.split("\\s+");
					String stationId = tokensLine[0];
					String latLng = tokensLine[0] + "-" + tokensLine[1];
					return new Tuple2<String, String>(stationId, latLng);
				});

		// Delete header from registers.csv, filter to remove inconsistent readings and
		// store the content into an RDD.

		JavaRDD<String> filteredRegistersRDD = registersRDD.filter(line -> {
			String[] tokens = line.split("\\s+");
			String stationId = tokens[0];
			int usedSlots = Integer.parseInt(tokens[3]);
			int freeSlots = Integer.parseInt(tokens[4]);
			return stationId != "stationId" && !(usedSlots == 0 && freeSlots == 0);
		});

		// Create a pair RDD in the format:
		// <stationId-slot> <criticality value>

		JavaPairRDD<String, String> stationSlotCriticalityRDD = filteredRegistersRDD.mapToPair(line -> {

			String[] lineTokens = line.split("\\s+");
			String stationId = lineTokens[0];
			String slot = DateTool.getTimeSlot(lineTokens[1]);
			String rddKey = stationId + "-" + slot;
			int freeSlots = Integer.parseInt(lineTokens[4]);

			// The first element represents the sum of 0 values and the second the items
			// counter.

			Counter rddValue = freeSlots == 0 ? new Counter(1, 1) : new Counter(0, 1);
			return new Tuple2<String, Counter>(rddKey, rddValue);

		}).reduceByKey((Counter c1, Counter c2) -> {

			int newSum = c1.getSum() + c2.getSum();
			int newCount = c1.getCount() + c2.getCount();
			return new Counter(newSum, newCount);

		}).mapToPair(element -> {
			double criticalSlotValue = (double) (element._2.getSum() / element._2.getCount());
			String stationId = element._1.split("-")[0];
			String rddValue = element._1.split("-")[1] + "-" + criticalSlotValue;
			return new Tuple2<>(stationId, rddValue);
		}).filter(element -> Double.valueOf(element._2.split("-")[1]) > criticalityTrashold);

		// Join the RDD

		JavaPairRDD<String, Tuple2<String, String>> finalRDD = stationSlotCriticalityRDD.join(stationsRDD);
		System.out.println("Output:\n" + finalRDD.collect());

		// Close the context
		context.close();

	}

}
