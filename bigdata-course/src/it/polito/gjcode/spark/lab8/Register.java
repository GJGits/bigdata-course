package it.polito.gjcode.spark.lab8;

import scala.Serializable;

@SuppressWarnings("serial")
public class Register implements Serializable {

	private int station;
	private String timeStamp;
	private int used_slots;
	private int free_slots;

	public Register() {
	}

	public int getStation() {
		return station;
	}

	public void setStation(int id) {
		this.station = id;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
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





}
