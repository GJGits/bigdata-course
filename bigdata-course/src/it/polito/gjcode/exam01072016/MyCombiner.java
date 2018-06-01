package it.polito.gjcode.exam01072016;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
			throws IOException, InterruptedException {

		double sum = 0.0;

		for (DoubleWritable doubleWritable : value) {
			sum += doubleWritable.get();
		}

		context.write(key, new DoubleWritable(sum));

	}

}
