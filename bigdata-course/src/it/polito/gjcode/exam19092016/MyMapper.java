package it.polito.gjcode.exam19092016;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String stationId = tokens[0];
		String year = tokens[1].split("/")[0];
		double pm10 = Double.parseDouble(tokens[4]);
		double pm25 = Double.parseDouble(tokens[5]);

		if (pm25 > pm10 && year == "2013")
			context.write(new Text(stationId), new IntWritable(1));

	}

}
