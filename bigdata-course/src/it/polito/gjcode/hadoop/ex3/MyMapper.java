package it.polito.gjcode.hadoop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private double threshold;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String sensorId = tokens[0];
		double pm10Value = Double.valueOf(tokens[2]);

		if (pm10Value > threshold)
			context.write(new Text(sensorId), new IntWritable(1));

	}

}
