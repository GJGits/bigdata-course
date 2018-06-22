package it.polito.gjcode.exam14072017;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String city = tokens[1];
		double max = Double.parseDouble(tokens[3]);
		double min = Double.parseDouble(tokens[4]);

		if (max > 35 && min > -20)
			context.write(new Text(city), new DoubleWritable(max));

		if (max < 35 && min < -20)
			context.write(new Text(city), new DoubleWritable(min));

		if (max > 35 && min < -20) {
			context.write(new Text(city), new DoubleWritable(max));
			context.write(new Text(city), new DoubleWritable(min));
		}

	}

}
