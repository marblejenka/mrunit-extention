package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.Job;

/**
 * Factory class to create instances of {@link PipelineMapReduceDriver}.
 */
public class PipelineMapReduceDriverFactory {
  /**
   * Private constructor to prevent instantiation.
   */
	private PipelineMapReduceDriverFactory(){
	}
	
  /**
   * Creates a {@link PipelineMapReduceDriver} configured with a sequence of
   * MapReduce stages based on the provided {@link Job} objects.
   * <p>
   * Each {@link Job} object in the {@code jobs} varargs is used to create a
   * {@link MapReduceDriver} stage using {@link MapReduceDriverFactory#createMapReduceDriver(Job)}.
   * These stages are then added in order to the pipeline.
   * </p>
   * <p>
   * Note: The returned {@link PipelineMapReduceDriver} instance is raw typed.
   * The user will need to cast it to the appropriate generic types based on the
   * initial input and final output types of the pipeline. For example:
   * <pre>{@code
   * PipelineMapReduceDriver<Text, Text, Text, Text> pipeline =
   *     (PipelineMapReduceDriver<Text, Text, Text, Text>) PipelineMapReduceDriverFactory.createMapReduceDriver(job1, job2);
   * }</pre>
   * </p>
   *
   * @param jobs A varargs array of Hadoop {@link Job} objects. Each Job represents
   *             a stage in the MapReduce pipeline.
   * @return A new {@link PipelineMapReduceDriver} (raw typed) instance with the
   *         configured pipeline stages.
   */
	@SuppressWarnings({ "rawtypes" })
	public static PipelineMapReduceDriver createMapReduceDriver(Job...jobs) {
		PipelineMapReduceDriver driver = new PipelineMapReduceDriver();
		
		for (Job job : jobs) {
			driver.addMapReduce(MapReduceDriverFactory.createMapReduceDriver(job));
		}
		return driver;
	}
}
