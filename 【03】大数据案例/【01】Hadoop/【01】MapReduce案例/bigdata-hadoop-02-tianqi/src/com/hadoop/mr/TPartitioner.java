package com.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 设置分区器，用于设置该组数据应该由哪一个Reduce进行计算
 * K V P
 * @author LGX
 *
 */
public class TPartitioner extends Partitioner<TQ, IntWritable>{

	@Override
	public int getPartition(TQ key, IntWritable value, int numPartitions) {
		return key.getYear() % numPartitions;
	}
}
