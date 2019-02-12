package com.hadoop.mr;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author LGX
 *
 */

public class FReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
 
	IntWritable rval = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		//相同的key为一组,这一组数据调用一次reduce方法
		//方法内迭代这一组数据
		
		int flg =0;
		int sum=0;
		for (IntWritable v : values) {
			if(v.get() == 0){
				flg = 1;
			}
			sum += v.get();
		}
		if(flg ==0 ){
			rval.set(sum);
			context.write(key, rval);
			
		}
		
	}
}

