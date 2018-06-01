package it.polito.gjcode.exam01072016;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path ouputPath = new Path(args[1]);
		int exitCode = 1;

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Hadoop esame 01-07-2016");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, ouputPath);
		job.setJarByClass(HadoopDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(3);

		if (job.waitForCompletion(true))
			exitCode = 0;

		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
		System.exit(res);
	}
}
