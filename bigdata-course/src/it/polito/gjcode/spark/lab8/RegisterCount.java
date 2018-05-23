package it.polito.gjcode.spark.lab8;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RegisterCount implements Serializable {

	private int stationId;
	private String slot;

	// this assume 1 for true (free_slots = 0) or 0 for false
	private int free;
	
	public RegisterCount() {
		// TODO Auto-generated constructor stub
	}
	
	public RegisterCount(Register register) {
		setStationId(register.getStation());
		setSlot(Tools.getTimeSlot(register.getTimeStamp()));
		int value = register.getFree_slots() == 0 ? 1 : 0;
		setFree(value);
	}
	

	public int getStationId() {
		return stationId;
	}

	public void setStationId(int stationId) {
		this.stationId = stationId;
	}

	public String getSlot() {
		return slot;
	}

	public void setSlot(String slot) {
		this.slot = slot;
	}

	public int getFree() {
		return free;
	}

	public void setFree(int yesOrNot) {
		this.free = yesOrNot;
	}

}
