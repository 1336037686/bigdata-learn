package com.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FirstPartitioner extends HashPartitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if(new Text("count").equals(key)) {
			return 3;
		}
		return super.getPartition(key, value, numPartitions - 1);
	}

}
