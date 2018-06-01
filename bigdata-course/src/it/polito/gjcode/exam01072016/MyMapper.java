package it.polito.gjcode.exam01072016;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String bookId = tokens[1];
		double price = Double.parseDouble(tokens[3]);

		context.write(new Text(bookId), new DoubleWritable(price));

	}

}
