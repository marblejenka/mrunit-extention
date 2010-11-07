package org.apache.hadoop.mrunit.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriverBase;
import org.apache.hadoop.mrunit.types.Pair;

public class MultipleInputsMapReduceDriver<K2 extends Comparable<?>, V2, K3, V3>
		extends
		MapReduceDriverBase<WritableComparable<?>, Writable, K2, V2, K3, V3> {

	public static final Log LOG = LogFactory
			.getLog(MultipleInputsMapReduceDriver.class);

	// private Mapper<WritableComparable<?>, Writable, K2, V2> myMapper;
	private Map<Mapper<WritableComparable<?>, Writable, K2, V2>, List<Pair<WritableComparable<?>, Writable>>> myMappers;
	private Reducer<K2, V2, K3, V3> myReducer;
	private Counters counters;

	public MultipleInputsMapReduceDriver(final Reducer<K2, V2, K3, V3> r) {
		myMappers = new HashMap<Mapper<WritableComparable<?>, Writable, K2, V2>, List<Pair<WritableComparable<?>, Writable>>>();
		myReducer = r;
		counters = new Counters();
	}

	public MultipleInputsMapReduceDriver() {
		counters = new Counters();
	}

	// /**
	// * Set the Mapper instance to use with this test driver
	// *
	// * @param m
	// * the Mapper instance to use
	// */
	// public void setMapper(Mapper<WritableComparable<?>, Writable, K2, V2> m)
	// {
	// myMapper = m;
	// }
	//
	// /** Sets the Mapper instance to use and returns self for fluent style */
	// public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withMapper(
	// Mapper<WritableComparable<?>, Writable, K2, V2> m) {
	// setMapper(m);
	// return this;
	// }

	/**
	 * @return the Mapper object being used by this test
	 */
	public Map<Mapper<WritableComparable<?>, Writable, K2, V2>, List<Pair<WritableComparable<?>, Writable>>> getMapperAndInputs() {
		return myMappers;
	}

	/**
	 * Sets the reducer object to use for this test
	 * 
	 * @param r
	 *            The reducer object to use
	 */
	public void setReducer(Reducer<K2, V2, K3, V3> r) {
		myReducer = r;
	}

	/**
	 * Identical to setReducer(), but with fluent programming style
	 * 
	 * @param r
	 *            The Reducer to use
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withReducer(
			Reducer<K2, V2, K3, V3> r) {
		setReducer(r);
		return this;
	}

	/**
	 * @return the Reducer object being used for this test
	 */
	public Reducer<K2, V2, K3, V3> getReducer() {
		return myReducer;
	}

	/** @return the counters used in this test */
	public Counters getCounters() {
		return counters;
	}

	/**
	 * Sets the counters object to use for this test.
	 * 
	 * @param ctrs
	 *            The counters object to use.
	 */
	public void setCounters(final Counters ctrs) {
		this.counters = ctrs;
	}

	/** Sets the counters to use and returns self for fluent style */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withCounters(
			final Counters ctrs) {
		setCounters(ctrs);
		return this;
	}

	@Override
	public void addInput(Pair<WritableComparable<?>, Writable> input) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addInput(WritableComparable<?> key, Writable val) {
		throw new UnsupportedOperationException();
	}

	public void addMapperAndInputs(
			Mapper<WritableComparable<?>, Writable, K2, V2> mapper,
			Pair<WritableComparable<?>, Writable>... inputs) {
		if (myMappers.containsKey(mapper)) {
			throw new IllegalStateException(
					"cannot atatch same mapper class for multiple inputs. this is not hadoop specification. this class has not support same mapper class testcase");
		}

		myMappers.put(mapper, Arrays.asList(inputs));
	}

	// /**
	// * Identical to addInput() but returns self for fluent programming style
	// *
	// * @param key
	// * @param val
	// * @return this
	// */
	// public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withInput(
	// WritableComparable<?> key, Writable val) {
	// addInput(key, val);
	// return this;
	// }

	/**
	 * Identical to addInput() but returns self for fluent programming style
	 * 
	 * @param input
	 *            The (k, v) pair to add
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withMapperAndInputs(
			Mapper<WritableComparable<?>, Writable, K2, V2> mapper,
			Pair<WritableComparable<?>, Writable>... inputs) {
		addMapperAndInputs(mapper, inputs);
		return this;
	}

	/**
	 * Works like addOutput(), but returns self for fluent style
	 * 
	 * @param outputRecord
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutput(
			Pair<K3, V3> outputRecord) {
		addOutput(outputRecord);
		return this;
	}

	/**
	 * Functions like addOutput() but returns self for fluent programming style
	 * 
	 * @param key
	 * @param val
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutput(K3 key,
			V3 val) {
		addOutput(key, val);
		return this;
	}

	// /**
	// * Identical to addInputFromString, but with a fluent programming style
	// *
	// * @param input
	// * A string of the form "key \t val". Trims any whitespace.
	// * @return this
	// */
	// public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withInputFromString(
	// String input) {
	// addInputFromString(input);
	// return this;
	// }

	/**
	 * Identical to addOutputFromString, but with a fluent programming style
	 * 
	 * @param output
	 *            A string of the form "key \t val". Trims any whitespace.
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutputFromString(
			String output) {
		addOutputFromString(output);
		return this;
	}

	public List<Pair<K3, V3>> run() throws IOException {

		List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();

		// run map component
		for (@SuppressWarnings("rawtypes")
		Mapper mapper : myMappers.keySet()) {
			LOG.debug("Running mapper " + mapper.toString() + ")");
			for (Pair<WritableComparable<?>, Writable> input : myMappers
					.get(mapper)) {
				LOG.debug(" Mapping input " + input.toString() + ")");
				mapOutputs
						.addAll(new MapDriver<WritableComparable<?>, Writable, K2, V2>()
								.withInput(input).withCounters(getCounters())
								.withConfiguration(configuration).run());
			}
		}

		List<Pair<K2, List<V2>>> reduceInputs = shuffle(mapOutputs);
		List<Pair<K3, V3>> reduceOutputs = new ArrayList<Pair<K3, V3>>();

		for (Pair<K2, List<V2>> input : reduceInputs) {
			K2 inputKey = input.getFirst();
			List<V2> inputValues = input.getSecond();
			StringBuilder sb = new StringBuilder();
			formatValueList(inputValues, sb);
			LOG.debug("Reducing input (" + inputKey.toString() + ", "
					+ sb.toString() + ")");

			reduceOutputs.addAll(new ReduceDriver<K2, V2, K3, V3>(myReducer)
					.withCounters(getCounters())
					.withConfiguration(configuration).withInputKey(inputKey)
					.withInputValues(inputValues).run());
		}

		return reduceOutputs;
	}

	@Override
	public String toString() {
		return "MultipleInputsMapReduceDriver (0.20+) (" + myMappers + ", "
				+ myReducer + ")";
	}

	/**
	 * @param configuration
	 *            The configuration object that will given to the mapper and
	 *            reducer associated with the driver
	 * @return this driver object for fluent coding
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withConfiguration(
			Configuration configuration) {
		setConfiguration(configuration);
		return this;
	}

	/**
	 * Identical to {@link #setKeyGroupingComparator(RawComparator)}, but with a
	 * fluent programming style
	 * 
	 * @param groupingComparator
	 *            Comparator to use in the shuffle stage for key grouping
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withKeyGroupingComparator(
			RawComparator<K2> groupingComparator) {
		setKeyGroupingComparator(groupingComparator);
		return this;
	}

	/**
	 * Identical to {@link #setKeyOrderComparator(RawComparator)}, but with a
	 * fluent programming style
	 * 
	 * @param orderComparator
	 *            Comparator to use in the shuffle stage for key value ordering
	 * @return this
	 */
	public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withKeyOrderComparator(
			RawComparator<K2> orderComparator) {
		setKeyOrderComparator(orderComparator);
		return this;
	}
}
