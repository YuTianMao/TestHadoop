package com.mao.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReduce extends Reducer<Weather, IntWritable, Text, IntWritable>{
	
	Text reduceKey = new Text();
	IntWritable reduceValue = new IntWritable();
	
	@Override
	protected void reduce(Weather key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		
		int flag = 0;
		int day = 0;
		for (IntWritable value : values) {
			
			if (flag==0) {
				//输出结果 按温度高到低排序,格式：年-月-日  温度
				reduceKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				reduceValue.set(key.getTemperature());
				day=key.getDay();
				context.write(reduceKey, reduceValue);
				flag++;
			}
			if (flag!=0 && day!=key.getDay()) {
				reduceKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				reduceValue.set(key.getTemperature());
				context.write(reduceKey, reduceValue);
				//这样是不是走了循环两次就停止程序？reducer的run方法的while方法会调nextKey，会先判空，就能处理下组数据。不是很理解
				break;
			}
			
		}
		
	}
	
}
