package it.polito.gjcode.spark.lab7;

public class Counter {

	private int sum;
	private int count;

	public Counter(int sum, int count) {
		this.sum = sum;
		this.count = count;
	}

	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
