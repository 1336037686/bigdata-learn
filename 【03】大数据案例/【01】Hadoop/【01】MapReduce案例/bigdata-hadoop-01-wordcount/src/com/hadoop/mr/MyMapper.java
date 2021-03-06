package com.hadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text word = new Text();
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while(stringTokenizer.hasMoreTokens()) {
			word.set(stringTokenizer.nextToken());
			context.write(word, one);
		}
	}

}
