package it.polito.gjcode.exam22012018;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private TreeMap<String, Integer> maxPurchased;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		maxPurchased = new TreeMap<>();
		maxPurchased.put("", 0);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		if (sum > maxPurchased.firstEntry().getValue()) {
			String k = maxPurchased.firstKey();
			maxPurchased.remove(k);
			maxPurchased.put(key.toString(), sum);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Text key = new Text(maxPurchased.firstKey());
		IntWritable value = new IntWritable(maxPurchased.firstEntry().getValue());
		context.write(key, value);
	}

}
