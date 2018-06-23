package it.polito.gjcode.exam14072017;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MaxTempCount  implements Serializable{

	double maxTemp;
	int count;

	public MaxTempCount(double maxTemp, int count) {
		this.maxTemp = maxTemp;
		this.count = count;
	}
	
	public String toString() {
		return new String(""+maxTemp/(double)count);
	}

}
