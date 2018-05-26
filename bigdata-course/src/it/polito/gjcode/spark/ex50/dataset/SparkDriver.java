package it.polito.gjcode.spark.ex50.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		// Create a spark session
		SparkSession session = SparkSession.builder().appName("[Spark app ex50 daatset version]").getOrCreate();

		// Create profile dataset
		Dataset<Profile> profileSet = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath).as(Encoders.bean(Profile.class));

		// Create the final set mapping the previous one.
		Dataset<FinalRecord> finalSet = profileSet.map(profile -> {
			String nameSurname = String.join(" ", profile.getName(), profile.getSurname());
			return new FinalRecord(nameSurname);
		}, Encoders.bean(FinalRecord.class));

		// Show set
		finalSet.show(20);

		// Store the result
		finalSet.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Stop the sessiom
		session.stop();

	}

}
