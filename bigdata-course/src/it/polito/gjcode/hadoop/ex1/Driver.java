package it.polito.gjcode.hadoop.ex1;

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
		int exitCode = 0;
		Configuration configuration = this.getConf();

		// Job configuration
		Job job = Job.getInstance(configuration);
		job.setJobName("[job1-ex1]");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJarByClass(Driver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Ogni chiave viene assegnata-gestita da un solo reducer, quindi il numero di
		// reducer da assegnare al job deve essere maggiore di 1 in quanto anche la fase
		// di reduce e' parallelizzabile e deve essere minore o uguale al numero di
		// parole distinte che esistono nell'input in quanto se si scegliesse un numero
		// superiore il delta in piu' di reducer rimarrebbe sicuramente inutilizzato.

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
