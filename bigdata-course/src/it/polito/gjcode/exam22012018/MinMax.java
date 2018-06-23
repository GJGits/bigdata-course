package it.polito.gjcode.exam22012018;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MinMax implements Serializable {
	public double min = Double.MAX_VALUE;
	public double max = Double.MIN_VALUE;

	public String toString() {
		return new String(max + "," + min);
	}
}
