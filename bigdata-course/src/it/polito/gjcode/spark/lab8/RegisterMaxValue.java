package it.polito.gjcode.spark.lab8;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RegisterMaxValue implements Serializable {

	private int stationId;
	private String slot;
	private double maxValue;

	public int getStationId() {
		return stationId;
	}

	public void setStationId(int stationId) {
		this.stationId = stationId;
	}

	public String getSlot() {
		return slot;
	}

	public void setSlot(String slot) {
		this.slot = slot;
	}

	public double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}

}
