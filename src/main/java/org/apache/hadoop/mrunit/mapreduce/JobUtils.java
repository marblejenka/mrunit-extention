package org.apache.hadoop.mrunit.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

public class JobUtils {
	private JobUtils(){
	}
	
	public static Mapper<?,?,?,?> newMapperInstanceFrom(Job job) {
		return ReflectionUtils.newInstance(configuredMapperClass(job), job.getConfiguration());
	}
	
	public static Class<? extends Mapper<?,?,?,?>> configuredMapperClass(Job job) {
		try {
			return job.getMapperClass();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Reducer<?,?,?,?> newReducerInstanceFrom(Job job) {
		return ReflectionUtils.newInstance(configuredReducerClass(job), job.getConfiguration());
	}
	
	public static Class<? extends Reducer<?,?,?,?>> configuredReducerClass(Job job) {
		try {
			return job.getReducerClass();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
		
	public static Counters countersFrom(Job job) {
		try {
			return job.getCounters();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
