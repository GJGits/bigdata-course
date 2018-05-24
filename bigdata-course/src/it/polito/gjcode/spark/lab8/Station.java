package it.polito.gjcode.spark.lab8;

import scala.Serializable;

@SuppressWarnings("serial")
public class Station implements Serializable {

	private int stationId;
	private double latitude, longitude;
	private String name;

	public Station() {
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLat(double lat) {
		this.latitude = lat;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double lng) {
		this.longitude = lng;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getStationId() {
		return stationId;
	}

	public void setStationId(int stationId) {
		this.stationId = stationId;
	}

}
