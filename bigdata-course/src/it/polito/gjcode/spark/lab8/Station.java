package it.polito.gjcode.spark.lab8;

import scala.Serializable;

@SuppressWarnings("serial")
public class Station implements Serializable {

	private int id;
	private double lat, lng;
	private String name;

	public Station() {
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	

}
