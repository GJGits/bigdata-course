package it.polito.gjcode.spark.lab8;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class Tools {

	public static String getTimeSlot(Timestamp timestamp) {

		LocalDateTime dateTime = timestamp.toLocalDateTime();
		DayOfWeek dayOfWeek = dateTime.getDayOfWeek();
		int hour = dateTime.getHour();

		return dayOfWeek + "-" + hour;
	}

	public static String getTimeSlot(String timeStamp) {

		String slot = "NO-SLOT-FOUND";
		String[] timeStampTokens = timeStamp.trim().split("\\t");

		if (timeStampTokens.length == 2) {

			String date = timeStampTokens[0];
			String time = timeStampTokens[1];
			LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
			LocalTime localTime = LocalTime.parse(time, DateTimeFormatter.ISO_LOCAL_TIME);
			String dayOfTheWeek = localDate.getDayOfWeek().toString();
			int hour = localTime.getHour();
			slot = String.join("-", dayOfTheWeek, String.valueOf(hour));
		}

		return slot;
	}

	public static void writeKmlFile(String path, List<String> kmlElements) {

		Configuration confHadoop = new Configuration();

		try {
			URI uri = URI.create(path);

			FileSystem file = FileSystem.get(uri, confHadoop);
			FSDataOutputStream outputFile = file.create(new Path(uri));

			BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

			// Header
			bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
			bOutFile.newLine();

			// Markers
			for (String lineKML : kmlElements) {
				bOutFile.write(lineKML);
				bOutFile.newLine();
			}

			// Footer
			bOutFile.write("</Document></kml>");
			bOutFile.newLine();

			bOutFile.close();
			outputFile.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

}
