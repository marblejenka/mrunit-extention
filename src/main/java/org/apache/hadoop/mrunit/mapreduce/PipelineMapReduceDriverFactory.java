package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class PipelineMapReduceDriverFactory {
	private PipelineMapReduceDriverFactory(){
	}
	
	@SuppressWarnings({ "rawtypes" })
	public static PipelineMapReduceDriver createMapReduceDriver(Job...jobs) {
		PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
		
		for (Job job : jobs) {
			driver.addMapReduce(MapReduceDriverFactory.createMapReduceDriver(job));
		}
		return driver;
	}
}
