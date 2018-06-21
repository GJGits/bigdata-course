package it.polito.gjcode.exam30062017;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String[] tokens = value.toString().split(",");
		String stock = tokens[0];
		String month = tokens[1].split("/")[1];
		String year = tokens[1].split("/")[0];
		double price = Double.parseDouble(tokens[3]);
		
		if(year == "2016")
			context.write(new Text(stock+"_"+month), new DoubleWritable(price));
	
	}

}
