package it.polito.gjcode.hadoop.esame1;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	private TreeMap<String, Integer> bestOne;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		bestOne = new TreeMap<>();
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		int maxValue = bestOne.isEmpty() ? 0 : bestOne.firstEntry().getValue().intValue();
		int sum = 0;
		String k = key.toString();

		for (IntWritable intWritable : value) {
			sum += intWritable.get();
		}

		if (bestOne.isEmpty()) {
			bestOne.put(k, sum);
		} else if (bestOne.firstEntry().getValue().intValue() > maxValue) {
			String toRemove = bestOne.firstKey();
			bestOne.remove(toRemove);
			bestOne.put(k, sum);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Text key = new Text(bestOne.firstKey());
		IntWritable value = new IntWritable(bestOne.firstEntry().getValue().intValue());
		context.write(key, value);
	}

}
