package it.polito.gjcode.hadoop.ex5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		String sensorId = tokens[0];
		double pmValue = Double.valueOf(tokens[2]);
		
		context.write(new Text(sensorId), new DoubleWritable(pmValue));
		
	}

}
