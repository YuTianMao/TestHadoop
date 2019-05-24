package com.mao.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroupingComparator extends WritableComparator{
	
	public WeatherGroupingComparator(){
		super(Weather.class,true);
	}
	Weather w1 = null;
	Weather w2 = null;
	
	//分组只根据年月（前面排序以根据年月温度排好）
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		w1 = (Weather) a;
		w2 = (Weather) b;
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
		if(c1 == 0){
			return Integer.compare(w1.getMonth(), w2.getMonth());
		}
		return c1;
	}
	
}
