package it.polito.gjcode.hadoop.ex5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path outpuPath = new Path(args[1]);
		int exitCode = 0;

		Configuration configuration = this.getConf();
		Job job = Job.getInstance(configuration);
		job.setJobName("[job1-ex5]");
		job.setJarByClass(Driver.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outpuPath);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

		// reducer phase is parallelizable.
		job.setNumReduceTasks(2);

		exitCode = job.waitForCompletion(true) ? 0 : 1;

		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}

}
