package it.polito.gjcode.exam260620182;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String climaxPath = args[0];
		double precThre = Double.parseDouble(args[1]);
		double winThre = Double.parseDouble(args[2]);
		String outputA = args[3];
		String outputB = args[4];

		SparkSession session = SparkSession.builder().getOrCreate();

		Dataset<Climate> climate = session.read().format("csv").option("header", true).option("inferSchema", true)
				.load(climaxPath).as(Encoders.bean(Climate.class)).filter(clim -> {

					String newDate = clim.getDate().replaceAll("/", "");
					return newDate.compareTo("20180401") >= 0 && newDate.compareTo("20180531") <= 0;

				});

		climate.printSchema();

		climate.createOrReplaceTempView("clima");

		session.udf().register("getHour", (String time) -> {
			String hour = time.split("\\:")[0];
			return hour;
		}, DataTypes.StringType);

		Dataset<AvgStep> avgs = session.sql(
				"select sid, getHour(time) as hour, avg(precipitation) as avgPrec, avg(speed)as avgSpeed from clima group by sid, getHour(time)")
				.as(Encoders.bean(AvgStep.class));

		Dataset<AvgStep> filtered = avgs.filter(avg -> {
			return avg.getAvgPrec() < precThre && avg.getAvgSpeed() < winThre;
		});

		Dataset<String> resultA = filtered.map(avg -> {
			String sid = avg.getSid();
			String hour = avg.getHour();
			String key = sid + "-" + hour;
			return key;
		}, Encoders.STRING());

		resultA.printSchema();

		resultA.show(3);
		// resultA.write().format("csv").option("header", true).option("inferSchema",
		// true).save(outputA);

		session.stop();
	}

}
