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
import org.apache.hadoop.mrunit.types.Pair;

/**
 * A MapReduce test driver for testing jobs that use
 * {@link org.apache.hadoop.mapreduce.lib.input.MultipleInputs}.
 *
 * <p>This driver allows you to associate different {@link Mapper} implementations
 * with different sets of input data. Each Mapper's input key-value types
 * must be {@link WritableComparable} and {@link Writable} respectively.
 * All Mappers must output the same intermediate key-value types (K2, V2),
 * which then become the input to the single {@link Reducer} (K2, V2).
 * The Reducer produces the final output (K3, V3).</p>
 *
 * <p><b>Constraints:</b></p>
 * <ul>
 *   <li>This driver cannot test scenarios where different Mappers emitting the
 *       same key-value types are used for different data sources if those Mappers
 *       are instances of the same class. Each Mapper instance is used as a
 *       unique key for its input data.</li>
 *   <li>When used in a {@link PipelineMapReduceDriver}, this driver can only be
 *       the first stage in the pipeline if it needs to handle multiple input sources.
 *   </li>
 * </ul>
 *
 * @param <K2> Intermediate key type: output of Mappers, input to Reducer. Must extend {@link Comparable}.
 * @param <V2> Intermediate value type: output of Mappers, input to Reducer.
 * @param <K3> Final output key type from the Reducer.
 * @param <V3> Final output value type from the Reducer.
 *
 * @author marblejenka (Original author)
 */
@SuppressWarnings("rawtypes")
public class MultipleInputsMapReduceDriver<K2 extends Comparable<?>, V2, K3, V3>
    extends MapReduceDriver<WritableComparable, Writable, K2, V2, K3, V3> {

  public static final Log LOG = LogFactory
      .getLog(MultipleInputsMapReduceDriver.class);

  // Stores Mapper instances mapped to their respective list of input pairs.
  // Input types for each mapper are fixed to WritableComparable and Writable.
  private Map<Mapper<? extends WritableComparable, ? extends Writable, K2, V2>, List<Pair<? extends WritableComparable, ? extends Writable>>> myMappers;
  private Reducer<K2, V2, K3, V3> myReducer;
  private Counters counters;

  /**
   * Constructs a MultipleInputsMapReduceDriver with a specified Reducer.
   * Mappers and their inputs must be added separately using
   * {@link #addMapperAndInputs(Mapper, Pair...)}.
   *
   * @param r The {@link Reducer} instance to use for the reduce phase.
   */
  public MultipleInputsMapReduceDriver(final Reducer<K2, V2, K3, V3> r) {
    myMappers = new HashMap<Mapper<? extends WritableComparable, ? extends Writable, K2, V2>, List<Pair<? extends WritableComparable, ? extends Writable>>>();
    myReducer = r;
    counters = new Counters();
  }

  /**
   * Constructs a MultipleInputsMapReduceDriver with no Mappers or Reducer initially.
   * The Reducer and Mappers (with their inputs) must be set using
   * {@link #withReducer(Reducer)} (or {@link #setReducer(Reducer)}) and
   * {@link #addMapperAndInputs(Mapper, Pair...)} respectively.
   */
  public MultipleInputsMapReduceDriver() {
    myMappers = new HashMap<Mapper<? extends WritableComparable, ? extends Writable, K2, V2>, List<Pair<? extends WritableComparable, ? extends Writable>>>();
    counters = new Counters();
  }

  /**
   * Retrieves the map of {@link Mapper} instances to their associated list of input {@link Pair}s.
   *
   * @return A map where keys are Mapper instances and values are lists of their input records.
   *         The input records are pairs of {@link WritableComparable} and {@link Writable}.
   */
  public Map<Mapper<? extends WritableComparable, ? extends Writable, K2, V2>, List<Pair<? extends WritableComparable, ? extends Writable>>> getMapperAndInputs() {
    return myMappers;
  }

  /**
   * Sets the {@link Reducer} instance to be used for this test.
   *
   * @param r The {@link Reducer} instance.
   */
  public void setReducer(Reducer<K2, V2, K3, V3> r) {
    myReducer = r;
  }

  /**
   * Sets the {@link Reducer} instance for this test and returns the driver for fluent configuration.
   *
   * @param r The {@link Reducer} instance.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withReducer(
      Reducer<K2, V2, K3, V3> r) {
    setReducer(r);
    return this;
  }

  /**
   * Retrieves the {@link Reducer} instance being used for this test.
   *
   * @return The {@link Reducer} instance.
   */
  public Reducer<K2, V2, K3, V3> getReducer() {
    return myReducer;
  }

  /**
   * @return The {@link Counters} used in this test.
   */
  public Counters getCounters() {
    return counters;
  }

  /**
   * Sets the {@link Counters} object to use for this test.
   *
   * @param ctrs The {@link Counters} object to use.
   */
  public void setCounters(final Counters ctrs) {
    this.counters = ctrs;
  }

  /**
   * Sets the {@link Counters} to use and returns self for fluent style.
   * @param ctrs The {@link Counters} object to use.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withCounters(
      final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * This method is not supported by {@code MultipleInputsMapReduceDriver}.
   * Inputs must be added with a specific Mapper using
   * {@link #addMapperAndInputs(Mapper, Pair...)}.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public void addInput(Pair<WritableComparable, Writable> input) {
    throw new UnsupportedOperationException("Inputs must be added with a specific Mapper for MultipleInputsMapReduceDriver. Use addMapperAndInputs().");
  }

  /**
   * This method is not supported by {@code MultipleInputsMapReduceDriver}.
   * Inputs must be added with a specific Mapper using
   * {@link #addMapperAndInputs(Mapper, Pair...)}.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public void addInput(WritableComparable key, Writable val) {
    throw new UnsupportedOperationException("Inputs must be added with a specific Mapper for MultipleInputsMapReduceDriver. Use addMapperAndInputs().");
  }

  /**
   * Associates a {@link Mapper} instance with a list of input records.
   * The input keys and values for this Mapper must be {@link WritableComparable}
   * and {@link Writable} respectively. The Mapper must output keys of type K2
   * and values of type V2.
   *
   * @param mapper The {@link Mapper} instance. Each mapper instance is treated as unique.
   * @param inputs Varargs array of input {@link Pair}s for this Mapper.
   *               Each pair should contain a {@link WritableComparable} key and a {@link Writable} value.
   * @throws IllegalStateException if the same Mapper instance is added multiple times.
   */
  public void addMapperAndInputs(
      Mapper<? extends WritableComparable, ? extends Writable, K2, V2> mapper,
      Pair<? extends WritableComparable, ? extends Writable>... inputs) {
    if (myMappers.containsKey(mapper)) {
      throw new IllegalStateException(
          "Cannot attach the same Mapper instance for multiple input sets. " +
          "Each Mapper instance must be unique if used for different inputs. " +
          "This is a limitation of this test driver, not Hadoop itself.");
    }

    myMappers
        .put((Mapper<? extends WritableComparable, ? extends Writable, K2, V2>) mapper,
            Arrays.asList(inputs));
  }

  /**
   * Associates a {@link Mapper} instance with a list of input records and
   * returns the driver for fluent configuration.
   * The input keys and values for this Mapper must be {@link WritableComparable}
   * and {@link Writable} respectively. The Mapper must output keys of type K2
   * and values of type V2.
   *
   * @param mapper The {@link Mapper} instance. Each mapper instance is treated as unique.
   * @param inputs Varargs array of input {@link Pair}s for this Mapper.
   *               Each pair should contain a {@link WritableComparable} key and a {@link Writable} value.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   * @throws IllegalStateException if the same Mapper instance is added multiple times.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withMapperAndInputs(
      Mapper<WritableComparable, Writable, K2, V2> mapper, // Note: Generics here are a bit more specific due to how it's typically called.
      Pair<WritableComparable, Writable>... inputs) {
    addMapperAndInputs(mapper, inputs);
    return this;
  }

  /**
   * Adds an expected output record from the {@link Reducer} and returns the
   * driver for fluent configuration.
   *
   * @param outputRecord The expected (key, value) {@link Pair}.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutput(
      Pair<K3, V3> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Adds an expected output key and value from the {@link Reducer} and returns
   * the driver for fluent configuration.
   *
   * @param key The expected output key.
   * @param val The expected output value.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutput(K3 key,
      V3 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Adds an expected output record from a tab-delimited string ("key \t val")
   * and returns the driver for fluent configuration. This forces K3 and V3 to Text.
   *
   * @param output A string of the form "key \t val". Trims any whitespace.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withOutputFromString(
      String output) {
    addOutputFromString(output);
    return this;
  }

  /**
   * Runs the MapReduce job:
   * <ol>
   *   <li>For each Mapper and its associated inputs, a temporary {@link MapDriver} is created and executed.</li>
   *   <li>The outputs from all MapDrivers are collected.</li>
   *   <li>These collected map outputs are then shuffled (grouped and sorted).</li>
   *   <li>The shuffled data is passed to a temporary {@link ReduceDriver} (configured with this driver's Reducer).</li>
   *   <li>The output from the ReduceDriver is returned.</li>
   * </ol>
   *
   * @return A list of (key, value) {@link Pair}s output by the {@link Reducer}.
   * @throws IOException If an I/O error occurs during execution.
   */
  @SuppressWarnings("unchecked")
  public List<Pair<K3, V3>> run() throws IOException {

    List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();

    // Run map component for each mapper
    for (Mapper mapper : myMappers.keySet()) {
      LOG.debug("Running mapper " + mapper.toString() + ")");
      List<Pair<? extends WritableComparable, ? extends Writable>> mapperInputs = myMappers.get(mapper);
      if (mapperInputs == null || mapperInputs.isEmpty()) {
        LOG.warn("No inputs for mapper " + mapper.toString() + ". Skipping.");
        continue;
      }

      // Create a temporary MapDriver for each mapper and its inputs
      MapDriver<WritableComparable, Writable, K2, V2> mapDriver =
          new MapDriver<WritableComparable, Writable, K2, V2>(mapper);
      mapDriver.withCounters(getCounters()); // Share counters
      if (this.getConfiguration() != null) {
        mapDriver.withConfiguration(this.getConfiguration());
      }

      for (Pair<? extends WritableComparable, ? extends Writable> input : mapperInputs) {
        LOG.debug(" Mapping input " + input.toString() + ")");
        // Add input to this temporary MapDriver and run it to get its outputs
        // It's more efficient to run per input if MapDriver supports it,
        // or accumulate inputs if it's one run per MapDriver.
        // The current MapDriver.run() processes all inputs added.
        // So, we create a new MapDriver for each input or ensure MapDriver resets.
        // For simplicity here, let's assume one MapDriver instance per Mapper is fine,
        // and it processes its list of inputs.
      }
      // Add all inputs to the map driver for this specific mapper
      mapDriver.withAll((List<Pair<WritableComparable, Writable>>) mapperInputs);
      mapOutputs.addAll(mapDriver.run());
    }

    if (myReducer == null) {
      LOG.warn("No Reducer provided; returning direct map outputs if applicable, " +
               "but this driver expects a Reducer for MapReduce behavior.");
      // This case is ambiguous for MapReduceDriver. Map-only jobs are usually
      // handled by MapDriver. If K2,V2 are same as K3,V3, we could cast.
      // For now, let's stick to the MapReduce path.
      if (mapOutputs.isEmpty() && expectedOutputs.isEmpty()) {
        return new ArrayList<Pair<K3,V3>>(); // No map output, no expected output, no reducer
      }
      // If there's no reducer, the concept of K3, V3 matching K2, V2 is implied.
      // This part of MapReduceDriver is usually overridden for map-only cases.
      // Given this is MultipleInputs*MapReduce*Driver, a reducer is expected.
      // Throwing an error or clearly documenting this behavior is important.
      // For now, proceeding as if a reducer must exist.
      if (mapOutputs.size() > 0 && !expectedOutputs.isEmpty() && myReducer == null) {
         // If we have map outputs and expect outputs but no reducer, it's an issue.
         // However, the base MapReduceDriver handles map-only if Reducer is null.
         // Let's rely on the base class shuffle and reduce (which will skip reduce phase if Reducer is null)
      }
    }


    List<Pair<K2, List<V2>>> reduceInputs = shuffle(mapOutputs);
    List<Pair<K3, V3>> reduceOutputs = new ArrayList<Pair<K3, V3>>();

    if (myReducer != null) {
      for (Pair<K2, List<V2>> input : reduceInputs) {
        K2 inputKey = input.getFirst();
        List<V2> inputValues = input.getSecond();
        StringBuilder sb = new StringBuilder();
        formatValueList(inputValues, sb);
        LOG.debug("Reducing input (" + inputKey.toString() + ", "
            + sb.toString() + ")");

        reduceOutputs.addAll(new ReduceDriver<K2, V2, K3, V3>(myReducer)
            .withCounters(getCounters()) // Share counters
            .withConfiguration(getConfiguration())
            .withInputKey(inputKey)
            .withInputValues(inputValues).run());
      }
    } else {
      // Handle map-only case: output of shuffle is the final output
      // K2, V2 must be equivalent to K3, V3
      LOG.debug("No reducer specified, shuffle output is the final output.");
      for (Pair<K2, List<V2>> pair : reduceInputs) {
        for (V2 val : pair.getSecond()) {
          reduceOutputs.add(new Pair<K3, V3>((K3)pair.getFirst(), (V3)val));
        }
      }
    }


    return reduceOutputs;
  }

  @Override
  public String toString() {
    return "MultipleInputsMapReduceDriver (0.20+) with " + myMappers.size() + " Mappers, Reducer: "
        + (myReducer == null ? "null" : myReducer.getClass().getName()) + ")";
  }

  /**
   * Sets the {@link Configuration} to be used for this test and returns the
   * driver for fluent configuration.
   *
   * @param configuration The {@link Configuration} object.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withConfiguration(
      Configuration configuration) {
    setConfiguration(configuration);
    return this;
  }

  /**
   * Sets the {@link RawComparator} for grouping keys during the shuffle phase
   * and returns the driver for fluent configuration.
   *
   * @param groupingComparator Comparator to use for key grouping.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withKeyGroupingComparator(
      RawComparator<K2> groupingComparator) {
    setKeyGroupingComparator(groupingComparator);
    return this;
  }

  /**
   * Sets the {@link RawComparator} for ordering keys during the shuffle phase
   * and returns the driver for fluent configuration.
   *
   * @param orderComparator Comparator to use for key ordering.
   * @return this {@link MultipleInputsMapReduceDriver} instance.
   */
  public MultipleInputsMapReduceDriver<K2, V2, K3, V3> withKeyOrderComparator(
      RawComparator<K2> orderComparator) {
    setKeyOrderComparator(orderComparator);
    return this;
  }
}
