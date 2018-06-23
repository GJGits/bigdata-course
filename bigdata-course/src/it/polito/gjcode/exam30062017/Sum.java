package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class Sum implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer value1, Integer value2) throws Exception {
		return Integer.valueOf(value1 + value2);
	}
}
