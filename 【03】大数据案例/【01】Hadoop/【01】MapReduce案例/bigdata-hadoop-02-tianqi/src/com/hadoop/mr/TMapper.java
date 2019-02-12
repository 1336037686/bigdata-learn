package com.hadoop.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable>{

	private TQ mKey = new TQ();
	private IntWritable mValue = new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//value -> 1992-11-12 12:21:11	24c
		try {
			String[] valueStrs = StringUtils.split(value.toString(), '\t');
			//处理时间
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			cal.setTime(sdf.parse(valueStrs[0]));
			mKey.setYear(cal.get(Calendar.YEAR));
			mKey.setMonth(cal.get(Calendar.MONTH) + 1);
			mKey.setDay(cal.get(Calendar.DAY_OF_MONTH));

			//设置温度
			int wd = Integer.parseInt(valueStrs[1].substring(0, valueStrs[1].length() - 1));
			mKey.setWd(wd);
			mValue.set(wd);
			
			//将数据写出
			context.write(mKey, mValue);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

}
