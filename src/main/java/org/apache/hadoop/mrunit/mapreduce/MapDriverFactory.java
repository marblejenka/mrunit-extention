package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class MapDriverFactory {
	private MapDriverFactory() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static MapDriver createMapDriver(Job job) {
		MapDriver driver = new MapDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setMapper(JobUtils.newMapperInstanceFrom(job));
		// TODO counterはrunningじゃないととれない
		// driver.setCounters(JobUtils.countersFrom(job));
		return driver;
	}
}
