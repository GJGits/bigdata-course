package it.polito.gjcode.hadoop.ex3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		String threshold = args[2];
		int exitCode = 0;
		
		Configuration configuration = this.getConf();
		configuration.set("threshold", threshold);
		Job job = Job.getInstance(configuration);
		job.setJobName("[job1-ex3]");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJarByClass(Driver.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// valgono le stesse considerazioni effettuate nell'esercizio 1
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
