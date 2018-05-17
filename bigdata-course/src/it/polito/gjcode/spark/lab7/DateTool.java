package it.polito.gjcode.spark.lab7;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class DateTool {

	public static String getTimeSlot(String timeStamp) {

		String[] timeStampTokens = timeStamp.split("\\s+");
		String date = timeStampTokens[0];
		String time = timeStampTokens[1];
		LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
		LocalTime localTime = LocalTime.parse(time, DateTimeFormatter.ISO_LOCAL_TIME);
		String dayOfTheWeek = localDate.getDayOfWeek().toString();
		int hour = localTime.getHour();
		return String.join("-", dayOfTheWeek, String.valueOf(hour));
	}

}
