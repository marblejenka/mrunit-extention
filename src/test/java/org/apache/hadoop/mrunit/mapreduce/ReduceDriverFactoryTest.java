package org.apache.hadoop.mrunit.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class ReduceDriverFactoryTest {

	@SuppressWarnings("rawtypes")
	@Test
	public void testCreateReduceDriverRetrieveReducer() throws IOException {
		Job job = new Job();
		job.setReducerClass(Reducer.class);
		
		ReduceDriver driver = ReduceDriverFactory.createReduceDriver(job);
		Reducer Reducer = driver.getReducer();
		
		assertThat(Reducer, is(instanceOf(Reducer.class)));
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testCreateReduceDriverRetrieveConfiguretion() throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("test", "test-value");
		
		Job job = new Job(configuration);
		
		ReduceDriver driver = ReduceDriverFactory.createReduceDriver(job);
		String parameter = driver.getConfiguration().get("test");
		
		assertThat(parameter, is("test-value"));
	}
}
