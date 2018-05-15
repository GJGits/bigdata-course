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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
		JavaRDD<String> stationsRDD = context.textFile(stationsPath);

		// Filter RDDs and store what we need
		JavaRDD<String> filteredRegistersRDD = registersRDD.filter(line -> {
			String[] tokens = line.split("\\s+");
			String stationId = tokens[0];
			int usedSlots = Integer.parseInt(tokens[3]);
			int freeSlots = Integer.parseInt(tokens[4]);
			return stationId != "stationId" && !(usedSlots == 0 && freeSlots == 0);
		});

		// TODO: rest of the application

		// Store in resultKML one String, representing a KML marker, for each station
		// with a critical timeslot
		// JavaRDD<String> resultKML = ;
		JavaRDD<String> resultKML = null;

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
