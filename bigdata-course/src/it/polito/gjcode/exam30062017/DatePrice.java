package it.polito.gjcode.exam30062017;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DatePrice implements Serializable {

	public String date;
	public Double price;

	public DatePrice(String date, Double price) {
		this.date = date;
		this.price = price;
	}
}
