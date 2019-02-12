package com.hadoop.mr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Client
 * @author LGX
 *
 */
public class MyFOF {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);
		
		job.setJobName("analyse-fof");
		job.setJarByClass(MyFOF.class);
		
		//Input Output
		Path inputPath = new Path("/data/FOF.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path("/data/analyse/");
		if(outputPath.getFileSystem(conf).exists(outputPath)) {
			outputPath.getFileSystem(conf).delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath );
		
		//MapTask
		job.setMapperClass(FMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//RedcuceTask
		job.setReducerClass(FReducer.class);
		
		job.waitForCompletion(true);
		
	}
}
