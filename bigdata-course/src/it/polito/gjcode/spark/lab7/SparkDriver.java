package it.polito.gjcode.spark.lab7;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;

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
				.filter(line -> line.split("\\t")[0] != "stationId").mapToPair(line -> {
					String[] tokensLine = line.split("\\t");
					String stationId = tokensLine[0];
					String latLng = tokensLine[1] + "-" + tokensLine[2];
					return new Tuple2<String, String>(stationId, latLng);
				});

		// Delete header from registers.csv, filter to remove inconsistent readings and
		// store the content into an RDD.

		JavaRDD<String> filteredRegistersRDD = registersRDD.filter(line -> {
			String[] tokens = line.split("\\t");
			if(line.startsWith("s"))
				return false;
			return tokens[2].trim() != "0" || tokens[3].trim() != "0";
		});

		// Create a pair RDD in the format:
		// <stationId-slot> <criticality value>

		JavaPairRDD<String, String> stationSlotCriticalityRDD = filteredRegistersRDD.mapToPair(line -> {

			String[] lineTokens = line.split("\\t");
			String stationId = lineTokens[0];
			String slot = Tools.getTimeSlot(lineTokens[1]);
			String rddKey = stationId + "-" + slot;
			int freeSlots = Integer.parseInt(lineTokens[3]);

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

		// Join the RDDs and create the kml file

		List<String> kmlElements = new LinkedList<>();

		JavaRDD<String> finalRDD = stationSlotCriticalityRDD.join(stationsRDD).map(element -> {

			String stationId = element._1;
			// element._2._1 = date-hour-criticalValue
			String slotDay = element._2._1.split("-")[0];
			String slotHour = element._2._1.split("-")[1];
			String criticalValue = element._2._1.split("-")[2];
			// element._2._2 = lat-lng
			String lat = element._2._2.split("-")[0];
			String lng = element._2._2.split("-")[1];

			String elementTemplate = "<Placemark><name>{0}</name><ExtendedData><Data\n"
					+ "name=\"DayWeek\"><value>{1}</value></Data><Data\n"
					+ "name=\"Hour\"><value>{2}</value></Data><Data\n"
					+ "name=\"Criticality\"><value>{3}</value></Data></ExtendedData><\n"
					+ "Point><coordinates>{4},{5}</coordinates></Point></Placemark>";

			String kmlElement = MessageFormat.format(elementTemplate, stationId, slotDay, slotHour, criticalValue, lat,
					lng);

			return kmlElement;
		});

		kmlElements.addAll(finalRDD.collect());
		Tools.writeKmlFile(outputNameFileKML, kmlElements);

		// Close the context
		context.close();

	}

}
