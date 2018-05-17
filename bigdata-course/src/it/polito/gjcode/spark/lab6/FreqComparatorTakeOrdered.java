package it.polito.gjcode.spark.lab6;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class FreqComparatorTakeOrdered implements Comparator<Tuple2<String, Integer>>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, Integer> pair1, Tuple2<String, Integer> pair2) {
		// Compare the number of occurrencies of the two pairs of products
		// (i.e., the second field)
		// We want to select the most frequent pairs. takeOrdered returns the top k
		// elements in
		// ascending order. Hence, we must invert the result of the comparison between
		// frequencies
		return -1 * (pair1._2().compareTo(pair2._2()));
	}

}