package it.polito.gjcode.datasetTest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		SparkSession session = SparkSession.builder().getOrCreate();

		Dataset<Row> originalDataFrame = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(inputPath);
		
		originalDataFrame.createOrReplaceTempView("people");
		originalDataFrame.show();
		Dataset<Row> maxEta = session.sql("select nome, max(eta) as maxEta from people group by nome");
		maxEta.show();
		maxEta.createOrReplaceTempView("max");
		Dataset<Row> result = session.sql("select people.nome, eta from people, max where max.nome = people.nome");
		result.show();
		

		session.stop();

	}

}
