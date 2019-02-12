package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyMR {
	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration(true);
		
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyMR.class);
	     
	     // Specify various job-specific parameters     
	     job.setJobName("sxt-wc");
	     
	     Path inputPath = new Path("/data/WordCount.txt");
	     FileInputFormat.addInputPath(job, inputPath);
	     
	     Path outputPath = new Path("/data/analyse");
	     if(outputPath.getFileSystem(conf).exists(outputPath)) {
	    	 outputPath.getFileSystem(conf).delete(outputPath, true);
	     }
	     
	     FileOutputFormat.setOutputPath(job, outputPath);
	     
	     
	     job.setMapperClass(MyMapper.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     job.setReducerClass(MyReducer.class);
	     
	     job.waitForCompletion(true);
		
	}
}
