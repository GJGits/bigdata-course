package it.polito.gjcode.exam12072016;

import java.io.Serializable;

public class Count implements Serializable {

	private static final long serialVersionUID = 1L;
	private int sum;
	private int count;

	public Count(int sum, int count) {
		this.setSum(sum);
		this.count = count;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

}
