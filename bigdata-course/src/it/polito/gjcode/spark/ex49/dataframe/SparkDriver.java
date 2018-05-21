package it.polito.gjcode.spark.ex49.dataframe;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession sparkSession = SparkSession.builder().appName("[Spark ex49-DataFrame version]").getOrCreate();

		// Create a dataframe from profile.csv
		
		DataFrameReader frameReader = sparkSession.read().format("csv").option("header", true).option("inferSchema",
				true);
		

		sparkSession.stop();

	}

}
