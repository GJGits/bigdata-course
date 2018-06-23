package it.polito.gjcode.exam22012018;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper #2
 */
class MapperBigData2 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> {// Output value type

	int highestNumPurchases;
	String bidHighestNumPurchases;

	@Override
	protected void setup(Context context) {
		highestNumPurchases = Integer.MIN_VALUE;
		bidHighestNumPurchases = null;
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Text(bidHighestNumPurchases), new IntWritable(highestNumPurchases));
		String[] fields = value.toString().split("\\t");

		String bid = fields[0];
		int numPurchases = Integer.parseInt(fields[1]);

		if (bidHighestNumPurchases == null || highestNumPurchases < numPurchases
				|| (highestNumPurchases == numPurchases && bid.compareTo(bidHighestNumPurchases) < 0)) {
			bidHighestNumPurchases = bid;
			highestNumPurchases = numPurchases;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(bidHighestNumPurchases), new IntWritable(highestNumPurchases));
	}

}
