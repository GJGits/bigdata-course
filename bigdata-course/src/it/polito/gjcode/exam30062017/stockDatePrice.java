package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class stockDatePrice implements PairFunction<String, String, Double> {

	@Override
	public Tuple2<String, Double> call(String line) throws Exception {
		// Input format: stockId,date,hour:minute,price

		// Extract one pair (stockId_date, price) for each input line

		String fields[] = line.split(",");
		String stockId = fields[0];
		String date = fields[1];
		Double price = new Double(fields[3]);

		return new Tuple2<String, Double>(stockId+"_"+date,new Double(price));
	}

}
