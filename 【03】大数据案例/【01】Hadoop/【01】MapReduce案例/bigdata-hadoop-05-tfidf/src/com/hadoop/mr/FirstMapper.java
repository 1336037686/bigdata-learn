package com.hadoop.mr;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//拆分数据:3823890235477306	一会儿带儿子去动物园约起～
		String[] vs = value.toString().trim().split("\t");
		if(vs.length >= 2) {
			String id = vs[0].trim();
			String content = vs[1].trim();
			
			//使用中文分词器分词
			StringReader sr = new StringReader(content);
			IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
			Lexeme word = null;
			while((word = ikSegmenter.next()) != null) {
				String w = word.getLexemeText();
				context.write(new Text(w + "_" + id), new IntWritable(1));
			}
			context.write(new Text("count"), new IntWritable(1));
		}else {
			System.out.println(value.toString() + "----------");
		}
		
	}

}
