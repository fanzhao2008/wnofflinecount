package com.zhaofan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class WnofflineMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


	protected void map(LongWritable key, Text value,
					   Context context) throws java.io.IOException ,InterruptedException {
		String lineString = value.toString();
		if(lineString.indexOf("请求来源IP:") !=-1)
		{
			String iPStr = lineString.substring(lineString.indexOf("请求来源IP:")+7);
			context.write(new Text(iPStr), new IntWritable(1));
		}
	};

}
