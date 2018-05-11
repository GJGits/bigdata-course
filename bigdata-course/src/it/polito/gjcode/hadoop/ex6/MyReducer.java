package it.polito.gjcode.hadoop.ex6;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double min = Double.MAX_VALUE;
		double max = 0.0;

		for (DoubleWritable doubleWritable : values) {
			double pmValue = doubleWritable.get();
			if (pmValue > max)
				max = pmValue;
			if (pmValue < min)
				min = pmValue;
		}

		context.write(key, new Text("max=" + max + "_min=" + min));

	}

}
