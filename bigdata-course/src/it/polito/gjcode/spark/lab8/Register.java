package it.polito.gjcode.spark.lab8;

import scala.Serializable;

@SuppressWarnings("serial")
public class Register implements Serializable {

	private int stationId;
	private String timeStamp;
	private int usedSlots;
	private int freeSlots;

	public Register() {
	}

	public int getId() {
		return stationId;
	}

	public void setStationId(int id) {
		this.stationId = id;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public int getUsedSlots() {
		return usedSlots;
	}

	public void setUsedSlots(int usedSlots) {
		this.usedSlots = usedSlots;
	}

	public int getFreeSlots() {
		return freeSlots;
	}

	public void setFreeSlots(int freeSlots) {
		this.freeSlots = freeSlots;
	}




}
