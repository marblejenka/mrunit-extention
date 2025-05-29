package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

/**
 * Factory class to create instances of {@link ReduceDriver}.
 */
public class ReduceDriverFactory {
  /**
   * Private constructor to prevent instantiation.
   */
	private ReduceDriverFactory() {
	}

  /**
   * Creates and configures a {@link ReduceDriver} from the provided {@link Job}.
   * The Reducer instance and Configuration are set based on the Job's settings.
   *
   * Note: Counters are not set from the Job in this factory method because
   * they are typically available only after a Job has been run.
   *
   * @param job The Hadoop {@link Job} object from which to configure the ReduceDriver.
   * @return A new {@link ReduceDriver} instance configured with the Reducer and
   *         Configuration from the Job.
   */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ReduceDriver createReduceDriver(Job job) {
		ReduceDriver driver = new ReduceDriver();
		driver.setConfiguration(job.getConfiguration());
		driver.setReducer(JobUtils.newReducerInstanceFrom(job));
		// TODO: Counters cannot be retrieved from the Job context before it is run.
		// driver.setCounters(JobUtils.countersFrom(job));
		return driver;
	}
}
