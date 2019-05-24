package com.mao.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.service.cli.thrift.THandleIdentifier;

//这个类型要实现序列化反序列化,比较器      WritableComparable是写，作用序列化             泛型Weather为了比较
public class Weather implements WritableComparable<Weather>{

	private int year ;
	private int month;
	private int day;
	private int temperature;  //温度
	
	
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

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.setYear(in.readInt());
		this.setMonth(in.readInt());
		this.setDay(in.readInt());
		this.setTemperature(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temperature);
	}

	@Override
	public int compareTo(Weather that) {
		//the value 0 if x == y;a value less than 0 if x < y; and a value greater than 0 if x > y
		int c1 = Integer.compare(this.getYear(), that.getYear());
		//如果年份相等，则比较月份
		if(c1==0){
			int c2 = Integer.compare(this.getMonth(), that.getMonth());
			if (c2==0) {
				return Integer.compare(this.getDay(), that.getDay());
			}
			return c2;
		}
		return c1;
	}

}
