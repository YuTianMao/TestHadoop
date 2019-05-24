package com.mao.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable>{
	
	//切记对象别再map方法中定义，map会循环防止new过多对象
	Weather mapKey = new Weather();
	IntWritable mapValue = new IntWritable();
	
	protected void setup(Mapper<LongWritable, Text, Weather, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	protected void cleanup(Mapper<LongWritable, Text, Weather, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//数据源数据样本  2018-12-11 10:00:00    12c   (注意时间跟温度隔着一个制表符)
		//          2018-12-20(空格符)22:10:00(制表符)16c 
		
		try {
			String line = value.toString();
			String[] strs = StringUtils.split(line, "\t"); //举例切割后的字符串数组：{2018-12-11 10:00:00，12c}
			
			//格式化时间
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(strs[0]);
			//创建Calendar(日历类)对象
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			
			mapKey.setYear(cal.get(Calendar.YEAR));
			//0表示1月，所以要加1才是月份
			mapKey.setMonth(cal.get(Calendar.MONTH)+1);
			//DAY_OF_MONTH这个月的第几天    DAY_OF_YEAR  DAY_OF_WEEK
			mapKey.setDay(cal.get(Calendar.DAY_OF_MONTH));
			
			//温度，把温度c切割掉
			String substring = strs[1].substring(0, strs[1].lastIndexOf("c"));
			int temperature = Integer.parseInt(substring);
			
			mapKey.setTemperature(temperature);
			mapValue.set(temperature);
			context.write(mapKey, mapValue);
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
}
