package org.apache.hadoop.mrunit.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

public class MapDriverFactoryTest {

	@SuppressWarnings("rawtypes")
	@Test
	public void testCreateMapDriverRetrieveMapper() throws IOException {
		Job job = new Job();
		job.setMapperClass(Mapper.class);
		
		MapDriver driver = MapDriverFactory.createMapDriver(job);
		Mapper mapper = driver.getMapper();
		
		assertThat(mapper, is(instanceOf(Mapper.class)));
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testCreateMapDriverRetrieveConfiguretion() throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("test", "test-value");
		
		Job job = new Job(configuration);
		
		MapDriver driver = MapDriverFactory.createMapDriver(job);
		String parameter = driver.getConfiguration().get("test");
		
		assertThat(parameter, is("test-value"));
	}
}
