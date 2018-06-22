package it.polito.gjcode.exam22012018;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper2 extends Mapper<Text, Text, Text, IntWritable> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		int val = Integer.parseInt(value.toString());
		context.write(key, new IntWritable(val));

	}

}
