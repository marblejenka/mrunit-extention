package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class MapReduceDriverFactory {
	private MapReduceDriverFactory(){
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static MapReduceDriver createMapReduceDriver(Job job) {
		MapReduceDriver driver = new MapReduceDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setMapper(JobUtils.newMapperInstanceFrom(job));
		driver.setReducer(JobUtils.newReducerInstanceFrom(job));
		driver.setKeyOrderComparator(job.getSortComparator());
		driver.setKeyGroupingComparator(job.getGroupingComparator());
		
		// TODO counterはrunningじゃないととれない
		// driver.setCounters(JobUtils.countersFrom(job));		
		return driver;
	}
}
