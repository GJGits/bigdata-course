package it.polito.gjcode.exam2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Tools {

	public static Iterator<String> getCriticalPeriods(Iterable<String> elements) {

		List<String> elementList = new ArrayList<>();
		List<String> criticalPeriods = new ArrayList<>();

		// add and sort elments

		for (String date : elements) {
			elementList.add(date);
		}

		Collections.sort(elementList);

		int count = 0;
		LocalDate lastCheckedDate = LocalDate.parse(elementList.get(0), DateTimeFormatter.ofPattern("MM-dd-yyyy"));

		for (String date : elementList) {

			LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("MM-dd-yyyy"));

			if (localDate.equals(lastCheckedDate.plusDays(1)))
				count++;
			else
				count = 0;

			if (count == 3)
				criticalPeriods.add(localDate.minusDays(3).format(DateTimeFormatter.ofPattern("MM-dd-yyyy")));

			lastCheckedDate = localDate;

		}

		return criticalPeriods.iterator();

	}

}
