package it.polito.gjcode.spark.lab7;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CountTotReadingsTotFull implements Serializable {

	public int numReadings;
	public int numFullReadings;

	public CountTotReadingsTotFull(int numReadings, int numFullReadings) {
		this.numReadings = numReadings;
		this.numFullReadings = numFullReadings;
	}

	public String toString() {
		return new String(this.numFullReadings + " full reading/" + this.numReadings + " total readings");
	}
}
