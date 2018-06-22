package it.polito.gjcode.exam22012018;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path outputPathA = new Path(args[1]);
		Path outputPathB = new Path(args[2]);
		int exitCode = 1;

		Configuration configuration = this.getConf();
		Job job = Job.getInstance(configuration);
		job.setJobName("hadoop [job1] esame 22-01-2018");
		job.setJarByClass(HadoopDriver.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPathA);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(3);

		if (job.waitForCompletion(true)) {

			Job job2 = Job.getInstance(configuration);
			job2.setJobName("hadoop [job2] esame 22-01-2018");
			job2.setJarByClass(HadoopDriver.class);
			FileInputFormat.addInputPath(job2, outputPathA);
			FileOutputFormat.setOutputPath(job2, outputPathB);
			job2.setMapperClass(MyMapper2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setReducerClass(MyReducer2.class);
			job2.setNumReduceTasks(1);

			if (job2.waitForCompletion(true)) {
				exitCode = 0;
			}

		}

		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
		System.exit(res);
	}

}
