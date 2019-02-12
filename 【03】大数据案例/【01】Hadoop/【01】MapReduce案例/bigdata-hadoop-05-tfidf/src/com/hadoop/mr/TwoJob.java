package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJobName("weibo-2");
			
			Path inputPath = new Path("/data/tfidf/output/weibo1");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputPath = new Path("/data/tfidf/output/weibo2");
			if(fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath );
			
			
			job.setMapperClass(TwoMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setCombinerClass(TwoReduce.class);
			job.setReducerClass(TwoReduce.class);
			
			boolean res = job.waitForCompletion(true);
			if(res) {
				System.out.println("Second Job Success...");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
