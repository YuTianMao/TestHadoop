package com.mao.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//原本是要求继承RawComparator，但是  RawComparator是接口不能继承，所以继承RawComparator的实现类WritableComparator
public class WeatherSort extends WritableComparator {
	
	//这一步为了拿key的对象
	public WeatherSort(){
		super(Weather.class,true);
	}
	
	//重写compare方法，底层调用会使用到key对象
	Weather w1 = null;
	Weather w2 = null;
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		w1 = (Weather) a;
		w2 = (Weather) b;
		
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
		if(c1 == 0){
			int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
			if (c2 == 0) {
				//倒序，所以加负号(为什么是比较温度而不是day呢？)
				return -Integer.compare(w1.getTemperature(), w2.getTemperature());
			}
			return c2;
		}
		
		return c1;
	}
}
