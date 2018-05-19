package it.polito.gjcode.spark.ex44;

import java.io.Serializable;

public class Counter implements Serializable{

	private static final long serialVersionUID = 1L;
	private int sum;
	private int count;
	
	public Counter(int sum, int count) {
		this.sum = sum;
		this.count = count;
	}

	public int getSum() {
		return sum;
	}

	public int getCount() {
		return count;
	}
	
	

}
