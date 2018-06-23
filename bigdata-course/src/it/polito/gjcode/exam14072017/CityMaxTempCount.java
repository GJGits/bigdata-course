package it.polito.gjcode.exam14072017;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CityMaxTempCount implements Serializable {

	String city;
	double maxTemp;
	int count;

	public CityMaxTempCount(String city, double maxTemp, int count) {
		this.city = city;
		this.maxTemp = maxTemp;
		this.count = count;
	}

}
