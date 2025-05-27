package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

/**
 * Factory class to create instances of {@link MapDriver}.
 */
public class MapDriverFactory {
  /**
   * Private constructor to prevent instantiation.
   */
	private MapDriverFactory() {
	}

  /**
   * Creates and configures a {@link MapDriver} from the provided {@link Job}.
   * The Mapper instance is created based on the Job's configuration.
   *
   * Note: Counters are not set from the Job in this factory method because
   * they are typically available only after a Job has been run.
   *
   * @param job The Hadoop {@link Job} object from which to configure the MapDriver.
   * @return A new {@link MapDriver} instance configured with the Mapper and
   *         Configuration from the Job.
   */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static MapDriver createMapDriver(Job job) {
		MapDriver driver = new MapDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setMapper(JobUtils.newMapperInstanceFrom(job));
		// TODO: Counters cannot be retrieved from the Job context before it is run.
		// driver.setCounters(JobUtils.countersFrom(job));
		return driver;
	}
}
