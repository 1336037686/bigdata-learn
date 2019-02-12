package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//获取当前切片
		FileSplit fs = (FileSplit) context.getInputSplit();
		//判断当前切片文件是否是part-r-00003
		if(!fs.getPath().getName().contains("part-r-00003")) {
			//豆浆_3823890201582094	3
			String[] vs = value.toString().trim().split("\t");
			if(vs.length >= 2) {
				String[] ss = vs[0].split("_");
				if(ss.length >= 2) {
					String w = ss[0];
					context.write(new Text(w), new IntWritable(1));
				} else {
					System.out.println(value.toString() + "--------");
				}
			}
		}
	}

}
