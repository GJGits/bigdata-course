package it.polito.gjcode.exam14072017;

import java.io.Serializable;

public class Count implements Serializable {

	private static final long serialVersionUID = 1L;
    private double sum;
    private int count;
    
    public Count(double sum, int count) {
		this.sum = sum;
		this.count = count;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
    
    
	
}
