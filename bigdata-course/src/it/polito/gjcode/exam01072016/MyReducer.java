package it.polito.gjcode.exam01072016;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
			throws IOException, InterruptedException {

		double sum = 0.0;

		for (DoubleWritable doubleWritable : value) {
			sum += doubleWritable.get();
		}

		if (sum >= 1000000)
			context.write(key, new DoubleWritable(sum));

	}

}
