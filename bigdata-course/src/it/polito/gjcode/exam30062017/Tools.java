package it.polito.gjcode.exam30062017;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class Tools {

	public static boolean firstOfTheWeek(String data) {
		LocalDate date = LocalDate.parse(data, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
		return weekOfTheYear(date) != weekOfTheYear(date.minusDays(1));
	}

	public static boolean lastOfTheWeek(String data) {
		LocalDate date = LocalDate.parse(data, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
		return weekOfTheYear(date) != weekOfTheYear(date.plusDays(1));
	}

	private static int weekOfTheYear(LocalDate date) {
		WeekFields weekFields = WeekFields.of(Locale.getDefault());
		return date.get(weekFields.weekOfWeekBasedYear());
	}

	public static int weekOfTheYear(String data) {
		LocalDate date = LocalDate.parse(data, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
		WeekFields weekFields = WeekFields.of(Locale.getDefault());
		return date.get(weekFields.weekOfWeekBasedYear());
	}

}
