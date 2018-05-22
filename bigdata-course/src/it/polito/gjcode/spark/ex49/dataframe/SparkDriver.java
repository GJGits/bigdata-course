package it.polito.gjcode.spark.ex49.dataframe;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession sparkSession = SparkSession.builder().appName("[Spark ex49-DataFrame version]").getOrCreate();

		// Create a dataframe from profile.csv

		DataFrameReader frameReader = sparkSession.read().format("csv").option("header", true).option("inferSchema",
				true);

		Dataset<Row> profilesDF = frameReader.load(inputPath);

		// Define a User Defined Function called AgeCategory(Integer age)
		// that returns a string associated with the Category of the user.
		// AgeCategory = "[(age/10)*10-(age/10)*10+9]"
		// e.g.,
		// 43 -> [40-49]
		// 39 -> [30-39]
		// 21 -> [20-29]
		// 17 -> [10-19]
		// ..

		sparkSession.udf().register("AgeCategory", (Integer age) -> {
			int min = (age / 10) * 10;
			int max = min + 1;
			return new String("[" + min + "-" + max + "]");
		}, DataTypes.StringType);

		// Define a DataFrame with the following schema:
		// |-- name: string (nullable = true)
		// |-- surname: string (nullable = true)
		// |-- rangeage: String (nullable = true)

		Dataset<Row> profilesDiscretizedAge = profilesDF.selectExpr("name", "surname", "AgeCategory(age) as rangeage");

		profilesDiscretizedAge.printSchema();
		profilesDiscretizedAge.show();

		// Save the result in the output folder
		// To save the results in one single file, we use the repartition method
		// to associate the Dataframe with one single partition (by setting the number
		// of
		// partition to 1).
		profilesDiscretizedAge.repartition(1).write().format("csv").option("header", true).save(outputPath);

		sparkSession.stop();

	}

}
