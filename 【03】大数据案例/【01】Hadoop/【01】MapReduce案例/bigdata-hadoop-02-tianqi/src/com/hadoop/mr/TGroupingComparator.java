package com.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 用于Reduce的归并排序
 * @author LGX
 *
 */
public class TGroupingComparator extends WritableComparator{

	public TGroupingComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ obj1 = (TQ) a;
		TQ obj2 = (TQ) b;
		int c1 = Integer.compare(obj1.getYear(), obj2.getYear());
		if(c1 == 0) {
			return Integer.compare(obj1.getMonth(), obj2.getMonth());
		}
		return c1;
	}
	
	

}
