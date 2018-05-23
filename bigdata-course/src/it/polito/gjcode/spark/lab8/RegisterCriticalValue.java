package it.polito.gjcode.spark.lab8;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RegisterCriticalValue implements Serializable {

	private int stationId;
	private String slot;
	private double criticalValue;

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

	public double getCriticalValue() {
		return criticalValue;
	}

	public void setCriticalValue(double criticalValue) {
		this.criticalValue = criticalValue;
	}

}
