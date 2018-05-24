package it.polito.gjcode.spark.lab8;

import java.sql.Timestamp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark driver for an sql solution of the lab
 * 
 * @author gjcode
 *
 */

public class SparkDriver2 {

	public static void main(String[] args) {

		// Store arguments

		String registersPath = args[0];
		String stationsPath = args[1];
		String outputPath = args[2];
		double criticalThreshold = Double.valueOf(args[3]);

		// Create spark session
		SparkSession session = SparkSession.builder().appName("[Spark app lab8 sql version]").getOrCreate();

		// Create Register and Station Datasets

		Dataset<Register> registerDataset = session.read().format("csv").option("header", true)
				.option("inferSchema", true).option("delimiter", "\\t").load(registersPath)
				.as(Encoders.bean(Register.class));

		registerDataset.show(50);

		Dataset<Station> stationDataset = session.read().format("csv").option("header", true)
				.option("inferSchema", true).option("delimiter", "\\t").load(stationsPath)
				.withColumnRenamed("id", "stationId").as(Encoders.bean(Station.class));

		stationDataset.show(50);

		// Create a user defined function to convert timestamp in a slot
		session.udf().register("timeToSlot", (Timestamp timestamp) -> Tools.getTimeSlot(timestamp),
				DataTypes.StringType);

		// Create a user defined finction to map free_slots
		session.udf().register("mapSlots", (Double free_slots) -> free_slots.doubleValue() > 0.0 ? 0.0 : 1.0,
				DataTypes.DoubleType);

		registerDataset.createOrReplaceTempView("rdt");

		Dataset<Register> mappedRegister = session.sql(
				"select station, timeToSlot(timestamp) as timestamp, used_slots, mapSlots(free_slots) as free_slots from rdt")
				.as(Encoders.bean(Register.class));

		mappedRegister.show(50);

		// Create a temp view on mappedRegister
		mappedRegister.createOrReplaceTempView("rgd");

		Dataset<RegisterCriticalValue> regCriticValue = session
				.sql("select station as stationId, timestamp as slot, avg(free_slots) as criticalValue "
						+ "from rgd  group by station, timestamp")
				.as(Encoders.bean(RegisterCriticalValue.class));

		regCriticValue.show(50);

		// Create a temp view on regCriticalValue
		regCriticValue.createOrReplaceTempView("rcv");

		// Stop session
		session.stop();

	}

}
