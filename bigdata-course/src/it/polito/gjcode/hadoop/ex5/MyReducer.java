package it.polito.gjcode.hadoop.ex5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		double sum = 0.0;
		double avg = 0.0;

		for (DoubleWritable doubleWritable : values) {
			sum += doubleWritable.get();
			count++;
		}

		avg = sum / count;
		context.write(key, new DoubleWritable(avg));

	}

}
