package it.polito.gjcode.exam30062017;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double min = Double.MAX_VALUE;
		double max = 0.0;
		double absDiff = 0.0;
		double percentage = 0.0;

		for (DoubleWritable value : values) {
			double dobValue = value.get();
			min = dobValue < min ? dobValue : min;
			max = dobValue > max ? dobValue : max;
		}

		absDiff = max - min;
		percentage = (max - min) / min;

		if (percentage > 0.05)
			context.write(new Text(key), new Text("\t" + absDiff + ", " + percentage));

	}

}
