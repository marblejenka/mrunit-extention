package org.apache.hadoop.mrunit.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MultipleInputsMapReduceDriverTest {
	Mapper<Text, Text, Text, Text> textMapper;
	Reducer<Text, Text, Text, Text> textReducer;

	Mapper<LongWritable, LongWritable, Text, Text> longMapper;
	Mapper<Text, Text, CustomWritable, CustomWritable> textToCustomWritableMapper;
	Reducer<CustomWritable, CustomWritable, CustomWritable, CustomWritable> customWritableReducer;

	@Before
	public void setup() {
		textMapper = new Mapper<Text, Text, Text, Text>();
		textReducer = new Reducer<Text, Text, Text, Text>();

		longMapper = new LongToText();
		textToCustomWritableMapper = new TextToCustomWritable();
		customWritableReducer = new Reducer<CustomWritable, CustomWritable, CustomWritable, CustomWritable>();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExecution() throws IOException {
		MultipleInputsMapReduceDriver<Text, Text, Text, Text> driver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();
		driver.addMapperAndInputs(textMapper, new Pair<Text, Text>(new Text(
				"key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")));
		driver.setReducer(textReducer);

		List<Pair<Text, Text>> run = driver.run();
		assertThat(run.size(), is(8));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleInputsExecution() throws IOException {
		MultipleInputsMapReduceDriver<Text, Text, Text, Text> driver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();

		driver.addMapperAndInputs(longMapper,
				new Pair<LongWritable, LongWritable>(new LongWritable(1),
						new LongWritable(2)));

		driver.addMapperAndInputs(textMapper, new Pair<Text, Text>(new Text(
				"key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")));

		driver.setReducer(textReducer);
		
		List<Pair<Text, Text>> run = driver.run();
		assertThat(run.size(), is(9));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleInputsPipelineExecution() throws IOException {
		MultipleInputsMapReduceDriver<Text, Text, Text, Text> first = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();

		first.addMapperAndInputs(longMapper,
				new Pair<LongWritable, LongWritable>(new LongWritable(1),
						new LongWritable(2)));

		first.addMapperAndInputs(textMapper, new Pair<Text, Text>(new Text(
				"key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")), new Pair<Text, Text>(
				new Text("key"), new Text("value")));

		first.setReducer(textReducer);

		MapReduceDriver<Text, Text, CustomWritable, CustomWritable, CustomWritable, CustomWritable> second = new MapReduceDriver<Text, Text, CustomWritable, CustomWritable, CustomWritable, CustomWritable>();
		second.setMapper(textToCustomWritableMapper);
		second.setReducer(customWritableReducer);
		
		PipelineMapReduceDriver<WritableComparable<?>, Writable, CustomWritable, CustomWritable> pipeline = new PipelineMapReduceDriver<WritableComparable<?>, Writable, CustomWritable, CustomWritable>();
		pipeline.addMapReduce(first);
		pipeline.addMapReduce(second);
				
		List<Pair<CustomWritable,CustomWritable>> run = pipeline.run();
		assertThat(run.size(), is(9));
	}

	// test support class
	public class LongToText extends
			Mapper<LongWritable, LongWritable, Text, Text> {
		protected void map(
				LongWritable key,
				LongWritable value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, LongWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text(Long.valueOf(key.get()).toString()),
					new Text(Long.valueOf(value.get()).toString()));
		};
	}

	public class TextToCustomWritable extends
			Mapper<Text, Text, CustomWritable, CustomWritable> {
		protected void map(
				Text key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<Text, Text, CustomWritable, CustomWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new CustomWritable(key), new CustomWritable(value));
		};
	}
}
