package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class HighestPrice implements Function2<Double, Double, Double> {

	@Override
	public Double call(Double price1, Double price2) throws Exception {
		if (price1.doubleValue() > price2.doubleValue()) {
			return new Double(price1);
		} else {
			return new Double(price2);
		}
	}
}
