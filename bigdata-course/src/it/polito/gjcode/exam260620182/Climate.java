package it.polito.gjcode.exam260620182;

import java.io.Serializable;
import java.sql.Timestamp;

public class Climate implements Serializable {

	private static final long serialVersionUID = 1L;
	private String sid;
	private String date;
	private String time;
	private double precipitation;
	private double speed;

	public Climate(String sid, String date, String time, double precipitation, double speed) {
		this.sid = sid;
		this.date = date;
		this.time = time;
		this.precipitation = precipitation;
		this.speed = speed;
	}

	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public double getPrecipitation() {
		return precipitation;
	}

	public void setPrecipitation(double precipitation) {
		this.precipitation = precipitation;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

}
