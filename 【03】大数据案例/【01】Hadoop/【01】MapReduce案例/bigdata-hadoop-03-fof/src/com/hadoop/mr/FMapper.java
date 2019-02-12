package com.hadoop.mr;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 * @author LGX
 *
 */
public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable ,Text ,Text, IntWritable>.Context context) throws IOException, InterruptedException{
		Text mkey = new Text();
		IntWritable mval = new IntWritable();
		String [] strs=StringUtils.split(value.toString(),' ');
		for ( int i=1;i<strs.length;i++){
			mkey.set(getFOF(strs[0],strs[i]));
			mval.set(0);
			
			context.write(mkey,mval);
			
			for(int j = i+1;j<strs.length;j++){
				mkey.set(getFOF(strs[i],strs[j]));
				mval.set(1);
				context.write(mkey,mval);
			}
			
		}
	}
 
	private static String getFOF(String s1, String s2) {
		
		if(s1.compareTo(s2) < 0){
			return s1 + " " + s2;
		}else{
			return s2 + " " + s1;
		}
 
	}
}
