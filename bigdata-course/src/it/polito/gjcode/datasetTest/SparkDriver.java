package it.polito.gjcode.datasetTest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		SparkSession session = SparkSession.builder().getOrCreate();

		session.read().format("csv").option("header", true).option("inferSchema", true).load(inputPath).printSchema();

		session.stop();

	}

}
