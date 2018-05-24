package it.polito.gjcode.spark.lab8;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class Register implements Serializable {

	private int station;
	private Timestamp timestamp;
	private double used_slots;
	private double free_slots;

	public int getStation() {
		return station;
	}

	public void setStation(int station) {
		this.station = station;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public double getUsed_slots() {
		return used_slots;
	}

	public void setUsed_slots(double used_slots) {
		this.used_slots = used_slots;
	}

	public double getFree_slots() {
		return free_slots;
	}

	public void setFree_slots(double free_slots) {
		this.free_slots = free_slots;
	}

}
