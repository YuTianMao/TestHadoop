package com.mao.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<Weather, IntWritable>{

	@Override
	public int getPartition(Weather key, IntWritable value, int arg2) {
		
		//没固定模板写法，下面这种情况很容易造成数据倾斜，不严谨。案例演示而已
		return key.getYear() % arg2;
		
	}


}
