package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main Client
 * 
 * @author LGX
 *
 */
public class MyTQ {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);
		job.setJobName("analyse-tq");
		job.setJarByClass(MyTQ.class);
		
		//Input Output
		Path inputPath = new Path("/data/TianQi.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path("/data/analyse");
		if(outputPath.getFileSystem(conf).exists(outputPath)) {
			outputPath.getFileSystem(conf).delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath );
		
		//MapTask
		job.setMapperClass(TMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(TPartitioner.class);
		job.setSortComparatorClass(TSortComparator.class);
		
		job.setCombinerClass(TCombiner.class);
		job.setCombinerKeyGroupingComparatorClass(TGroupingComparator.class);
		
		//ReduceTask
		job.setGroupingComparatorClass(TGroupingComparator.class);
		job.setReducerClass(TReducer.class);
		
		job.waitForCompletion(true);
		
	}
}
