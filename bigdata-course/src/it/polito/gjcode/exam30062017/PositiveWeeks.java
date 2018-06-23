package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class PositiveWeeks implements Function<Tuple2<String, Iterable<DatePrice>>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Iterable<DatePrice>> inputPair) throws Exception {

		DatePrice firstDay = null;
		DatePrice lastDay = null;

		Boolean firstElement = true;

		// Iterate over the dates-prices of the current stockId-weekNumber to
		// select the first and the last days of the week, and the corresponding
		// max temperatures
		for (DatePrice dp : inputPair._2()) {
			// First element of the iterable
			if (firstElement == true) {
				firstDay = new DatePrice(dp.date, dp.price);
				lastDay = new DatePrice(dp.date, dp.price);

				firstElement = false;
			} else {
				if (dp.date.compareTo(firstDay.date) < 0) {
					firstDay = new DatePrice(dp.date, dp.price);
				}

				if (dp.date.compareTo(lastDay.date) > 0) {
					lastDay = new DatePrice(dp.date, dp.price);
				}
			}
		}

		// Check if the week is "positive"
		if (lastDay.price.doubleValue() > firstDay.price.doubleValue()) {
			return true;
		} else {
			return false;
		}
	}

}
