package it.polito.gjcode.spark.lab8;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Record implements Serializable {

	private int stationId;
	private String slot;
	private double criticality, longitude, latitude;

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

	public double getCriticality() {
		return criticality;
	}

	public void setCriticality(double criticality) {
		this.criticality = criticality;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

}
