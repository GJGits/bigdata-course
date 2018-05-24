package it.polito.gjcode.spark.lab8;

import java.io.Serializable;

public class Register implements Serializable {

	private static final long serialVersionUID = -3855122252759937251L;
	private int station;
	private String timestamp;
	private int used_slots;
	private int free_slots;

	public int getStation() {
		return station;
	}

	public void setStation(int station) {
		this.station = station;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public int getUsed_slots() {
		return used_slots;
	}

	public void setUsed_slots(int used_slots) {
		this.used_slots = used_slots;
	}

	public int getFree_slots() {
		return free_slots;
	}

	public void setFree_slots(int free_slots) {
		this.free_slots = free_slots;
	}

	@Override
	public String toString() {
		return String.join("\\t", String.valueOf(station), timestamp, String.valueOf(used_slots),
				String.valueOf(free_slots));
	}

}
