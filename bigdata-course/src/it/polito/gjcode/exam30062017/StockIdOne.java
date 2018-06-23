package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StockIdOne implements PairFunction<Tuple2<String, Iterable<DatePrice>>, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(Tuple2<String, Iterable<DatePrice>> inputPair) throws Exception {
		String stockId_WeekNumber = inputPair._1();
		
		String fields[] = stockId_WeekNumber.split("_");
		
		String stockId = fields[0];
		
		return new Tuple2<String, Integer>(new String(stockId), new Integer(1));
	}

}
