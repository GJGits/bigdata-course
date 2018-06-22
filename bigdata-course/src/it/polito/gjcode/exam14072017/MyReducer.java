package it.polito.gjcode.exam14072017;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		boolean hasMaxValue = false, hasMinValue = false;

		for (DoubleWritable value : values) {
			double doubleValue = value.get();
			if (doubleValue > 35)
				hasMaxValue = true;
			if (doubleValue < -20)
				hasMinValue = true;
		}

		if (hasMaxValue && hasMinValue)
			context.write(key, NullWritable.get());

	}

}
