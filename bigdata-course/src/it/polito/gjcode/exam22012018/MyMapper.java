package it.polito.gjcode.exam22012018;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String tokens[] = value.toString().split(",");
		String bId = tokens[1];
		String year = tokens[2];

		if (year.startsWith("2016"))
			context.write(new Text(bId), new IntWritable(1));

	}

}
