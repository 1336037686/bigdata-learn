package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 用于将Reduce移动到Map端进行一次先行计算，用于提高计算效率
 * @author LGX
 *
 */
public class TCombiner extends Reducer<TQ, IntWritable, TQ, IntWritable>{

	private TQ rKey = new TQ();
	private IntWritable rValue = new IntWritable();
	
	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Reducer<TQ, IntWritable, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int flg = 0;
		int day = 0;
		for (IntWritable intWritable : values) {
			if(flg == 0) {
				rKey = key;
				rValue.set(key.getWd());
				context.write(rKey, rValue);
				flg++;
				day = key.getDay();
			}
			if(flg != 0 && day != key.getDay()) {
				rKey = key;
				rValue.set(key.getWd());
				context.write(rKey, rValue);
				break;
			}
		}
	}

}
