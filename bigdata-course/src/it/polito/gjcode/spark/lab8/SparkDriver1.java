package it.polito.gjcode.spark.lab8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
//import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Column;

// **** DATASET SOLUTION ****

public class SparkDriver1 {

	public static void main(String[] args) {

		// Store arguments

		String registersPath = args[0];
		String stationsPath = args[1];
		String outputPath = args[2];
		double criticalThreshold = Double.valueOf(args[3]);

		// Create spark session
		SparkSession session = SparkSession.builder().appName("[Spark app lab8]").getOrCreate();

		Dataset<Register> registersDataset = session.read().format("csv").option("delimiter", "\\t")
				.option("header", true).option("inferSchema", true).load(registersPath)
				.as(Encoders.bean(Register.class));

		Dataset<Register> filteredRegister = registersDataset.filter(element -> {
			if (element.getFree_slots() == 0 && element.getUsed_slots() == 0)
				return false;
			return true;
		});
		
		//
		// Dataset<RegisterCount> mappedRegister = filteredRegister.map(e -> {
		//
		// RegisterCount registerCount = new RegisterCount();
		// registerCount.setSlot(Tools.getTimeSlot(e.getTimestamp()));
		// registerCount.setStationId(e.getStation());
		// int free = e.getFree_slots() == 0 ? 1 : 0;
		// registerCount.setFree(free);
		// return registerCount;
		//
		// }, Encoders.bean(RegisterCount.class));

		// Dataset<Station> stationDataset =
		// session.read().format("csv").option("header", true)
		// .option("inferSchema", true).option("delimiter", "\\t").load(stationsPath)
		// .as(Encoders.bean(Station.class));
		//
		// // Group by stationId and slot to create:
		// // | stationId | slot | criticalValue |
		//
		// Dataset<RegisterCriticalValue> regCriticValue =
		// registersDataset.groupBy("stationId", "slot").agg(avg("free"))
		// .withColumnRenamed("avg(free)",
		// "criticalValue").as(Encoders.bean(RegisterCriticalValue.class));
		//
		// Dataset<RegisterMaxValue> regMaxValue =
		// regCriticValue.groupBy("stationId").agg(max("criticalValue"))
		// .withColumnRenamed("max(criticalValue)",
		// "maxValue").as(Encoders.bean(RegisterMaxValue.class))
		// .filter(el -> el.getMaxValue() > criticalThreshold);
		//
		// // Create the final dataset with join operation.
		// // Output format:
		// // | stationId | criticalSlot | criticalValue | longitude | latitude |
		//
		// Dataset<Record> finalDataset = regMaxValue.join(stationDataset,
		// "stationId").as(Encoders.bean(Record.class))
		// .sort(new Column("maxValue").desc());
		//
		// // Write the final dataset as csv file
		// finalDataset.repartition(1).write().format("csv").option("header",
		// true).save(outputPath);

		filteredRegister.repartition(1).write().format("csv").option("header", true).save(outputPath);

		// Stop session
		session.stop();

	}

}
