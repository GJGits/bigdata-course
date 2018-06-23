package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StockWeek_DatePrice implements PairFunction<Tuple2<String, Double>, String, DatePrice> {

	@Override
	public Tuple2<String, DatePrice> call(Tuple2<String, Double> input) throws Exception {

		// Input pair: (stockId_date, highest_price)
		// New pair: ((stockId_weekNumber, date_highestPrice)

		String keys[] = input._1().split("_");
		String stockId = keys[0];
		String date = keys[1];
		Integer weekNum = DateTool.weekNumber(date);

		Double price = input._2();
		DatePrice dp = new DatePrice(date, price);

		return new Tuple2<String, DatePrice>(stockId + "_" + weekNum, dp);
	}

}
