package it.polito.gjcode.spark.lab7;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		JavaRDD<String> stationsRDD = context.textFile(stationsPath)
				.filter(line -> line.split("\\s+")[0] != "stationId");

		// Filter RDDs and store what we need

		JavaRDD<String> filteredRegistersRDD = registersRDD.filter(line -> {
			String[] tokens = line.split("\\s+");
			String stationId = tokens[0];
			int usedSlots = Integer.parseInt(tokens[3]);
			int freeSlots = Integer.parseInt(tokens[4]);
			return stationId != "stationId" && !(usedSlots == 0 && freeSlots == 0);
		});

		// Create a pairRDD in the format:
		// register -> <stationId-slot> <Iterable of freeSlots>

		JavaPairRDD<String, Iterable<Integer>> registerPairsRDD = filteredRegistersRDD.mapToPair(line -> {

			String[] lineTokens = line.split("\\s+");
			String stationId = lineTokens[0];
			String slot = DateTool.getTimeSlot(lineTokens[1].trim());
			int freeSlots = Integer.parseInt(lineTokens[4]);

			return new Tuple2<String, Integer>(stationId + slot, freeSlots);
		}).groupByKey();

		// Create an RDD in the format:
		// <stationId-slot> <criticality>

		JavaPairRDD<String, Double> stationSlotCriticalityRDD = registerPairsRDD.mapToPair(element -> {

			double elementsCounter = 0.0;
			double criticality = 0.0;
			// count the reads with a value of 0 free slots
			double numberOfZeroReadings = 0.0;

			for (Integer freeSlots : element._2) {
				if (freeSlots.intValue() == 0)
					numberOfZeroReadings++;
				elementsCounter++;
			}

			criticality = numberOfZeroReadings / elementsCounter;

			return new Tuple2<String, Double>(element._1, criticality);
		}).filter(element -> element._2 > criticalityTrashold);

		// Create a pairRDD in the format:
		// <stationId> <Iterable<slot-criticality>>

		JavaPairRDD<String, Iterable<String>> stationSlotCriticalitiesRDD = stationSlotCriticalityRDD
				.mapToPair(element -> {

					String[] tokens = element._1.split("-");
					String stationId = tokens[0];
					String slot = tokens[1];

					return new Tuple2<String, String>(stationId, String.join("-", slot, String.valueOf(element._2)));
				}).groupByKey();

		// Create the final register pair:
		// <stationId> <slot-value>

		JavaPairRDD<String, String> criticalSlotRDD = stationSlotCriticalitiesRDD.mapToPair(element -> {

			double maxCrit = 0.0;
			String maxSlot = "";

			for (String slotCritic : element._2) {
				String[] tokens = slotCritic.split("-");
				String slot = tokens[0];
				double critic = Double.parseDouble(tokens[1]);
				if (critic > maxCrit) {
					maxCrit = critic;
					maxSlot = slot;
				}
			}

			return new Tuple2<String, String>(element._1, String.join("-", maxSlot, String.valueOf(maxCrit)));
		});

		// Create the stations pair RDD
		JavaPairRDD<String, String> stationsPairRDD = stationsRDD.mapToPair(line -> {

			String[] tokens = line.split("\\s+");
			String stationId = tokens[0];
			String lat = tokens[1];
			String lng = tokens[2];
			String nome = tokens[4];

			return new Tuple2<String, String>(stationId, String.join("-", lat, lng, nome));

		});

		// Store in resultKML one String, representing a KML marker, for each station
		// with a critical timeslot
		// JavaRDD<String> resultKML = ;
		JavaRDD<String> resultKML = criticalSlotRDD.join(stationsPairRDD).map(element -> {

			String stationId = element._1;
			String[] registerTokens = element._2._1.split("-");
			String[] stationTokens = element._2._2.split("-");
			String day = registerTokens[0].split("-")[0];
			String hour = registerTokens[0].split("-")[1];
			String criticValue = registerTokens[1];
			String lat = stationTokens[0];
			String lng = stationTokens[1];
			String nome = stationTokens[2];

			String kmlElement = "<Placemark><name>" + stationId + "</name><ExtendedData><Data\n"
					+ "name=\"DayWeek\"><value>" + day + "</value></Data>><Data\n" + "name=\"Hour\"><value>" + hour
					+ "</value></Data><Data\n" + "name=\"Criticality\"><value>" + criticValue
					+ "</value></Data></ExtendedData><\n" + "Point><coordinates>" + lat + "," + lng
					+ "</coordinates></Point></Placemark>";

			return kmlElement;
		});

		// There is at most one string for each station. We can use collect and
		// store the returned list in the main memory of the driver.
		List<String> localKML = resultKML.collect();

		// Store the result in one single file stored in the distributed file
		// system
		// Add header and footer, and the content of localKML in the middle
		Configuration confHadoop = new Configuration();

		try {
			URI uri = URI.create(outputNameFileKML);

			FileSystem file = FileSystem.get(uri, confHadoop);
			FSDataOutputStream outputFile = file.create(new Path(uri));

			BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

			// Header
			bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
			bOutFile.newLine();

			// Markers
			for (String lineKML : localKML) {
				bOutFile.write(lineKML);
				bOutFile.newLine();
			}

			// Footer
			bOutFile.write("</Document></kml>");
			bOutFile.newLine();

			bOutFile.close();
			outputFile.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Close the context
		context.close();

	}

}
