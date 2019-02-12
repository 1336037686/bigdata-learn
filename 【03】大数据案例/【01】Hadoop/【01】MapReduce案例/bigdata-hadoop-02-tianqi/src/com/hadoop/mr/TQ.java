package com.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 天气基本类型
 * 1.需要年，月，日，温度四个属性
 * 2.需要实现 WritableComparable<T> 接口
 * 3.需要重写序列化，反序列化，比较器方法
 * @author LGX
 *
 */
public class TQ implements WritableComparable<TQ>{

	private int year;
	private int month;
	private int day;
	private int wd;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeInt(this.wd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	/**
	 * 按照year,month,day正序排列
	 */
	@Override
	public int compareTo(TQ that) {
		int c1 = Integer.compare(this.year, that.getYear());
		if(c1 == 0) {
			int c2 = Integer.compare(this.month, that.getMonth());
			if(c2 == 0) {
				return Integer.compare(this.day, that.getDay());
			}
			return c2;
		}
		return c1;
	}
}
