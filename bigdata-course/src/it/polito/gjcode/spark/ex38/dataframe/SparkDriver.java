package it.polito.gjcode.spark.ex38.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #38 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: sensorId,date,PM10 value
		// (μg/m3 )\n
		// The input file has no header. Hence, the name of the columns of
		// DataFrame will be _c0, _c1, _c2
		Dataset<Row> dfReadings = ss.read().format("csv").option("header", false).option("inferSchema", true)
				.load(inputPath);

		// Filter the content of dsReadings. Select only the lines with PM10>50 (i.e., _c2>50)
		Dataset<Row> dfReadingsCritical = dfReadings.filter("_c2>50");

		
		// Group data by sensorid (column _c0)
		RelationalGroupedDataset rgdReadingsPerSensor = dfReadingsCritical.groupBy("_c0");

		// For each sensor, apply the count aggregate function
		// Compute the count() for each group.
		// Cast the returned DataFrame to a Dataset<SensorCount>
		Dataset<Row> countPerSensorDF = rgdReadingsPerSensor.count()
				.withColumnRenamed("_c0", "sensorid");
		
		// Select only the records with count>=2
		Dataset<Row> countPerSensorDFFrequent = countPerSensorDF.filter("count>=2");

		// Store the result in the output folder
		countPerSensorDFFrequent.write().format("csv").save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
