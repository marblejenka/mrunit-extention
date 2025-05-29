package org.apache.hadoop.mrunit.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utility methods for working with Hadoop {@link Job} objects.
 */
public class JobUtils {
  /**
   * Private constructor to prevent instantiation.
   */
	private JobUtils(){
	}
	
  /**
   * Creates a new {@link Mapper} instance from the given {@link Job}.
   *
   * @param job The Hadoop Job.
   * @return A new Mapper instance.
   */
	public static Mapper<?,?,?,?> newMapperInstanceFrom(Job job) {
		return ReflectionUtils.newInstance(configuredMapperClass(job), job.getConfiguration());
	}
	
  /**
   * Gets the configured {@link Mapper} class from the given {@link Job}.
   *
   * @param job The Hadoop Job.
   * @return The configured Mapper class.
   * @throws RuntimeException if the Mapper class is not found.
   */
	public static Class<? extends Mapper<?,?,?,?>> configuredMapperClass(Job job) {
		try {
			return job.getMapperClass();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
  /**
   * Creates a new {@link Reducer} instance from the given {@link Job}.
   *
   * @param job The Hadoop Job.
   * @return A new Reducer instance.
   */
	public static Reducer<?,?,?,?> newReducerInstanceFrom(Job job) {
		return ReflectionUtils.newInstance(configuredReducerClass(job), job.getConfiguration());
	}
	
  /**
   * Gets the configured {@link Reducer} class from the given {@link Job}.
   *
   * @param job The Hadoop Job.
   * @return The configured Reducer class.
   * @throws RuntimeException if the Reducer class is not found.
   */
	public static Class<? extends Reducer<?,?,?,?>> configuredReducerClass(Job job) {
		try {
			return job.getReducerClass();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
		
  /**
   * Gets the {@link Counters} from the given {@link Job}.
   *
   * @param job The Hadoop Job.
   * @return The Counters from the Job.
   * @throws RuntimeException if an IOException occurs while retrieving counters.
   */
	public static Counters countersFrom(Job job) {
		try {
			return job.getCounters();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
