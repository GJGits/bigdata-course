package it.polito.gjcode.exam1;

public class Tools {

	public static int getDurationView(String start, String end) {

		int startHour = Integer.parseInt(start.split("\\:")[0]);
		int startMin = Integer.parseInt(start.split("\\:")[1]);
		int endHour = Integer.parseInt(end.split("\\:")[0]);
		int endMin = Integer.parseInt(end.split("\\:")[1]);

		return (endHour * 60 + endMin) - (startHour * 60 + startMin);
	}
	
}
