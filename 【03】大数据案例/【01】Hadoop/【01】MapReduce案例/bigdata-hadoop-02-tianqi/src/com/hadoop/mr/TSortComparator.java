package com.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 设置Map端快速排序的排序规则
 * 设置排序比较器
 * 按照年月正序排列，按照温度逆序排列
 * @author LGX
 *
 */
public class TSortComparator extends WritableComparator{

	public TSortComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ obj1 = (TQ) a;
		TQ obj2 = (TQ) b;
		int c1 = Integer.compare(obj1.getYear(), obj2.getYear());
		if(c1 == 0) {
			int c2 = Integer.compare(obj1.getMonth(), obj2.getMonth());
			if(c2 == 0) {
				return -Integer.compare(obj1.getWd(), obj2.getWd());
			}
			return c2;
		}
		return c1;
	}

}
