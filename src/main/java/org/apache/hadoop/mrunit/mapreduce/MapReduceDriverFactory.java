package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

/**
 * Factory class to create instances of {@link MapReduceDriver}.
 */
public class MapReduceDriverFactory {
  /**
   * Private constructor to prevent instantiation.
   */
	private MapReduceDriverFactory(){
	}
	
  /**
   * Creates and configures a {@link MapReduceDriver} from the provided {@link Job}.
   * The Mapper, Reducer, Configuration, sort comparator, and grouping comparator
   * are all configured from the Job object.
   *
   * Note: Counters are not set from the Job in this factory method because
   * they are typically available only after a Job has been run.
   *
   * @param job The Hadoop {@link Job} object from which to configure the MapReduceDriver.
   * @return A new {@link MapReduceDriver} instance configured with the Mapper,
   *         Reducer, Configuration, and comparators from the Job.
   */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static MapReduceDriver createMapReduceDriver(Job job) {
		MapReduceDriver driver = new MapReduceDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setMapper(JobUtils.newMapperInstanceFrom(job));
		driver.setReducer(JobUtils.newReducerInstanceFrom(job));
		driver.setKeyOrderComparator(job.getSortComparator());
		driver.setKeyGroupingComparator(job.getGroupingComparator());
		
		// TODO: Counters cannot be retrieved from the Job context before it is run.
		// driver.setCounters(JobUtils.countersFrom(job));		
		return driver;
	}
}
