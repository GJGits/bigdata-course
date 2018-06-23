package it.polito.gjcode.exam14092017;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {
	public int numFullyBooked = 0;
	public int numCancelled = 0;
	public int numFlights = 0;

	public String toString() {
		return new String(numFullyBooked + " " + numCancelled + " " + numFlights);
	}
}
