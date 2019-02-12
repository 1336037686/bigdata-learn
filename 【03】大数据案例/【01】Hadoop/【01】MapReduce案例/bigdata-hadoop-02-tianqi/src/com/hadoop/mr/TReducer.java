package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends Reducer<TQ, IntWritable, Text, Text>{
	private Text rKey = new Text();
	private Text rValue = new Text();
	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Reducer<TQ, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int flg = 0;
		int day = 0;
		for (IntWritable intWritable : values) {
			if(flg == 0) {
				rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				rValue.set(key.getWd() + "c");
				context.write(rKey, rValue);
				flg++;
				day = key.getDay();
			}
			if(flg != 0 && day != key.getDay()) {
				rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				rValue.set(key.getWd() + "c");
				context.write(rKey, rValue);
				break;
			}
		}
	}
	
	
}
