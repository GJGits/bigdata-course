package it.polito.gjcode.exam12072016;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String zone = tokens[2];
		String city = tokens[3];
		int slots = Integer.parseInt(tokens[4]);

		if (slots > 20 && city == "Turin")
			context.write(new Text(zone), new IntWritable(1));

	}

}
