package it.polito.gjcode.exam2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		double pm10Reading = Double.parseDouble(tokens[2]);

		if (pm10Reading > 45.0 || pm10Reading < 0.0)
			context.write(NullWritable.get(), value);

	}

}
