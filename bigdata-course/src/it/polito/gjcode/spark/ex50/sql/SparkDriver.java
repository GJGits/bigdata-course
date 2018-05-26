package it.polito.gjcode.spark.ex50.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		// Create a session
		SparkSession session = SparkSession.builder().appName("[Spark app ex50 sql version]").getOrCreate();

		Dataset<Profile> profileSet = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath).as(Encoders.bean(Profile.class));

		// Create a temp view
		profileSet.createOrReplaceTempView("profile");

		// Create final set
		Dataset<FinalRecord> finalSet = session.sql("select concat(name, surname) as name_surname from profile")
				.as(Encoders.bean(FinalRecord.class));

		// Show result
		finalSet.show(20);

		// Store the result
		finalSet.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Stop session
		session.stop();

	}

}
