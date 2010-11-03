package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class ReduceDriverFactory {
	private ReduceDriverFactory() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ReduceDriver createReduceDriver(Job job) {
		ReduceDriver driver = new ReduceDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setReducer(JobUtils.newReducerInstanceFrom(job));
		// TODO counterはrunningじゃないととれない
		// driver.setCounters(JobUtils.countersFrom(job));
		return driver;
	}
}
