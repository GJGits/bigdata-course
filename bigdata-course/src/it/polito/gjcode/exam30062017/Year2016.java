package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class Year2016 implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) throws Exception {

		// Input format: stockId,date,hour:minute,price

		String fields[] = line.split(",");

		String date = fields[1];
		String year = date.split("/")[0];

		// Select data of year 2016
		if (year.compareTo("2016") == 0) {
			return true;
		} else {
			return false;
		}
	}

}
