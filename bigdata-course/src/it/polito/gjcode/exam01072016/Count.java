package it.polito.gjcode.exam01072016;

import java.io.Serializable;

public class Count implements Serializable {

	private static final long serialVersionUID = -1586345129422513736L;
	private int sum, count;

	public Count(int sum, int count) {
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

	public int getSum2() {
		return sum;
	}

	public void setSum2(int sum2) {
		this.sum = sum2;
	}

	public int getCount2() {
		return count;
	}

	public void setCount2(int count2) {
		this.count = count2;
	}

}
