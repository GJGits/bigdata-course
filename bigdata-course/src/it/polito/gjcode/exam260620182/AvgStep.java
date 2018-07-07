package it.polito.gjcode.exam260620182;

import java.io.Serializable;

public class AvgStep implements Serializable {

	private static final long serialVersionUID = 1L;
	private String sid;
	private String hour;
	private double avgPrec, avgSpeed;
    
	public AvgStep(String sid, String hour) {
		this.sid = sid;
		this.hour = hour;
	}

	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public double getAvgPrec() {
		return avgPrec;
	}

	public void setAvgPrec(double avgPrec) {
		this.avgPrec = avgPrec;
	}

	public double getAvgSpeed() {
		return avgSpeed;
	}

	public void setAvgSpeed(double avgSpeed) {
		this.avgSpeed = avgSpeed;
	}
	
	
	
	
}
