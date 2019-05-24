package com.mao.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MyWeather {
	
	public static void main(String[] args){
		
		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf);
			job.setJarByClass(MyWeather.class);
			
			//input,output
			Path input = new Path("/usr/input/weather.txt");
			Path output = new Path("/output/data/weather");
			
			FileSystem fs = FileSystem.get(conf);
			FileInputFormat.addInputPath(job, input);
			
			//map task
			job.setMapperClass(WeatherMapper.class);
			//自定义key分组类型  
			job.setMapOutputKeyClass(Weather.class);
			job.setMapOutputValueClass(IntWritable.class);
			//map分区器，这样就不使用默认的分区器了
			job.setPartitionerClass(WeatherPartitioner.class);
			//自定义排序比较器
			job.setSortComparatorClass(WeatherSort.class);
			
			//reduce task
			//分组比较器
			job.setGroupingComparatorClass(WeatherGroupingComparator.class);
			job.setReducerClass(WeatherReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//submit
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
