package it.polito.gjcode.spark.ex49.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession session = SparkSession.builder().appName("[Spark app ex49 dataset version]").getOrCreate();

		// create a dataset of profile
		Dataset<Profile> profileSet = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath).as(Encoders.bean(Profile.class));

		// create category range
		Dataset<ProfileNewAge> profileWithNewAge = profileSet.map(element -> {

			ProfileNewAge profRange = new ProfileNewAge(element);
			return profRange;

		}, Encoders.bean(ProfileNewAge.class));

		// save the result as a csv file
		profileWithNewAge.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// stop the session
		session.stop();
	}

}
