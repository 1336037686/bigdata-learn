package com.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	private IntWritable result = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		int sum = 0;
		while(iterator.hasNext()) {
			sum += iterator.next().get();
		}
		result.set(sum);
		context.write(key, result);
		
		
	}

	
}

