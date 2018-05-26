package it.polito.gjcode.spark.ex49.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession session = SparkSession.builder().appName("[Spark app ex 49 sql-version]").getOrCreate();

		// Create profile dataset
		Dataset<Profile> profileSet = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath).as(Encoders.bean(Profile.class));

		// Create a temp view on profileSet called 'profile'
		profileSet.createOrReplaceTempView("profile");

		// Create the user defined function rangeAge
		session.udf().register("rangeAge", (Integer age) -> {
			int min = (age / 10) * 10;
			int max = (age / 10) * 10 + 9;
			String newAge = "[" + min + "-" + max + "]";
			return newAge;
		}, DataTypes.StringType);

		// Create the final dataset using a query on the temp view
		Dataset<ProfileNewAge> profNewAge = session.sql("select name, surname, rangeAge(age) as rangeAge from profile")
				.as(Encoders.bean(ProfileNewAge.class));

		// Show the result
		profNewAge.show(20);

		// Store the result in a file
		profNewAge.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Stop session
		session.stop();

	}

}
