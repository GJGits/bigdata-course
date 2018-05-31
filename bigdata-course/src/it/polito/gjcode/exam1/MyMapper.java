package it.polito.gjcode.exam1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		String movieId = tokens[1];
		String start = tokens[2].split("_")[1];
		String end = tokens[3].split("_")[1];
		int duration = Tools.getDurationView(start, end);

		if (duration > 10)
			context.write(new Text(movieId), new IntWritable(1));

	}

}
