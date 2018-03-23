package com.zhaofan;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WnofflineCount {
	public static void main(String[] args) throws Exception {
		//定义一个job
		Job job = new Job();
		job.setJarByClass(WnofflineCount.class);
		//设置job的名称
		job.setJobName("Max Temp");
		//添加输入文件，可以是文件或者文件夹。
		//文件夹不进行递归，目前测试含有子文件夹就会报错
		//文件夹中可以是压缩文件，如gz，会自动解压
		//可以多次设置
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//设置输出目录，只能设置一次，并且文件夹应该不存在，否则报错
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置mapper类
		job.setMapperClass(WnofflineMapper.class);
		//设置reduces类
		job.setReducerClass(WnoflineReducer.class);
		//设置输出的key类型
		job.setOutputKeyClass(Text.class);
		//设置输出的value类型
		job.setOutputValueClass(IntWritable.class);
		//执行，并打印执行结果
		System.out.println(job.waitForCompletion(true));



	}

}
