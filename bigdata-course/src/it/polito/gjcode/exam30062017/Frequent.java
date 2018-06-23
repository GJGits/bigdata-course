package it.polito.gjcode.exam30062017;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class Frequent implements Function<Tuple2<String, Integer>, Boolean> {

	Integer nw;

	public Frequent(int nw) {
		this.nw = nw;
	}

	@Override
	public Boolean call(Tuple2<String, Integer> inputPair) throws Exception {
		if (inputPair._2().compareTo(nw) >= 0) {
			return true;
		} else {
			return false;
		}
	}

}