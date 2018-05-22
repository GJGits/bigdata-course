package it.polito.gjcode.spark.lab8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkDriver1 {

	public static void main(String[] args) {

		// Store arguments

		String registersPath = args[0];
		String stationsPath = args[1];
		String outputPath = args[2];
		double criticalThreshold = Double.valueOf(args[3]);

		// Create spark session
		SparkSession session = SparkSession.builder().appName("[Spark app lab8]").getOrCreate();

		// Create datasets

		Dataset<Register> registersDataset = session.read().format("csv").option("header", true)
				.option("inferSchema", true).load(registersPath).as(Encoders.bean(Register.class)).filter(element -> {
					return !(element.getFreeSlots() == 0 && element.getUsedSlots() == 0);
				}).map(e -> {
					e.setTimeStamp(Tools.getTimeSlot(e.getTimeStamp()));
					return e;
				}, Encoders.bean(Register.class));

		Dataset<Station> stationDataset = session.read().format("csv").option("header", true)
				.option("inferSchema", true).load(stationsPath).as(Encoders.bean(Station.class));

		// Create a DataFrame in the format:
		// | stationId | slot | count(slot) |

		registersDataset.groupBy("stationId, timestamp").agg(functions.count("*"));

		// Stop session
		session.stop();

	}

}
