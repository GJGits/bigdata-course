package it.polito.gjcode.exam1;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class MovieUser implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String line) throws Exception {

		String[] fields = line.split(",");
		Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

		return movieUser;
	}

}
