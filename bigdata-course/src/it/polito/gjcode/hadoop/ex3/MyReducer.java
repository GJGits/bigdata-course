package it.polito.gjcode.hadoop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;

		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}

		context.write(key, new IntWritable(count));

	}

}
