package org.apache.hadoop.mrunit.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness for testing a pipeline of MapReduce jobs. Allows for the linear
 * chaining of multiple {@link MapReduceDriver}s. The output of one stage of
 * the pipeline becomes the input of the next.
 *
 * <p>This class is designed to make it easy to test complex MapReduce workflows
 * that involve multiple processing stages. Users can add {@link MapReduceDriver}
 * instances to the pipeline, provide initial input, and then verify the final
 * output after all stages have been executed.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * PipelineMapReduceDriver<Text, Text, Text, Text> pipeline =
 *     new PipelineMapReduceDriver<Text, Text, Text, Text>();
 *
 * // Configure first MapReduce job
 * MapReduceDriver<Text, Text, Text, Text> mr1 =
 *     new MapReduceDriver<Text, Text, Text, Text>()
 *         .withMapper(new MyMapper1())
 *         .withReducer(new MyReducer1());
 *
 * // Configure second MapReduce job
 * MapReduceDriver<Text, Text, Text, Text> mr2 =
 *     new MapReduceDriver<Text, Text, Text, Text>()
 *         .withMapper(new MyMapper2())
 *         .withReducer(new MyReducer2());
 *
 * pipeline.addMapReduce(mr1);
 * pipeline.addMapReduce(mr2);
 *
 * pipeline.withInput(new Text("key1"), new Text("value1"));
 * pipeline.withOutput(new Text("finalKey"), new Text("finalValue"));
 * pipeline.runTest();
 * }
 * </pre>
 *
 * @param <K1> The initial input key type for the first Mapper in the pipeline.
 * @param <V1> The initial input value type for the first Mapper in the pipeline.
 * @param <K2> The final output key type from the last Reducer in the pipeline.
 * @param <V2> The final output value type from the last Reducer in the pipeline.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PipelineMapReduceDriver<K1, V1, K2, V2>
    extends TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(PipelineMapReduceDriver.class);

  private List<MapReduceDriver> mapReducePipeline;
  private List<Pair<K1, V1>> inputList;
  private Counters counters;

  /**
   * Constructs a PipelineMapReduceDriver with an initial sequence of
   * MapReduce jobs.
   *
   * @param mapReducePipeline A varargs array of {@link MapReduceDriver} instances
   *                          that make up the initial pipeline.
   */
  public PipelineMapReduceDriver(final MapReduceDriver... mapReducePipeline) {
    this.mapReducePipeline = new ArrayList<MapReduceDriver>();
    this.mapReducePipeline.addAll(Arrays.asList(mapReducePipeline));
    this.inputList = new ArrayList<Pair<K1, V1>>();
    this.counters = new Counters();
  }

  /**
   * Constructs an empty PipelineMapReduceDriver. MapReduce jobs can be added
   * later using {@link #addMapReduce(MapReduceDriver)} or
   * {@link #withMapReduce(MapReduceDriver)}.
   */
  public PipelineMapReduceDriver() {
    this.mapReducePipeline = new ArrayList<MapReduceDriver>();
    this.inputList = new ArrayList<Pair<K1, V1>>();
    this.counters = new Counters();
  }

  /**
   * Retrieves the {@link Counters} used throughout the execution of the pipeline.
   * Counters are accumulated across all MapReduce jobs in the pipeline.
   *
   * @return The {@link Counters} object containing metrics from the test run.
   */
  public Counters getCounters() {
    return counters;
  }

  /**
   * Sets the {@link Counters} object to use for this test. This allows for
   * injecting a specific Counters instance, for example, to pre-populate
   * counter values or to use a mock Counters object.
   *
   * @param ctrs The {@link Counters} object to use.
   */
  public void setCounters(final Counters ctrs) {
    this.counters = ctrs;
  }

  /**
   * Sets the {@link Counters} to use for this test and returns the driver instance
   * for fluent programming.
   *
   * @param ctrs The {@link Counters} object to use.
   * @return this {@link PipelineMapReduceDriver} instance.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Adds a {@link MapReduceDriver} instance, configured with a Mapper and a Reducer,
   * to the end of the pipeline.
   *
   * @param m The {@link Mapper} instance for this stage of the pipeline.
   * @param r The {@link Reducer} instance for this stage of the pipeline.
   */
  public void addMapReduce(Mapper m, Reducer r) {
    addMapReduce(new MapReduceDriver(m, r));
  }

  /**
   * Adds a pre-configured {@link MapReduceDriver} instance to the end of the pipeline.
   *
   * @param driver The {@link MapReduceDriver} instance to add.
   */
  public void addMapReduce(MapReduceDriver driver) {
    this.mapReducePipeline.add(driver);
  }

  /**
   * Adds a {@link MapReduceDriver} instance (configured with a Mapper and a Reducer)
   * to the end of the pipeline and returns the driver instance for fluent programming.
   *
   * @param m The {@link Mapper} instance for this stage.
   * @param r The {@link Reducer} instance for this stage.
   * @return this {@link PipelineMapReduceDriver} instance.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(Mapper m, Reducer r) {
    addMapReduce(m, r);
    return this;
  }

  /**
   * Adds a pre-configured {@link MapReduceDriver} instance to the end of the
   * pipeline and returns the driver instance for fluent programming.
   *
   * @param p The {@link MapReduceDriver} instance to add.
   * @return this {@link PipelineMapReduceDriver} instance.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(MapReduceDriver p) {
    addMapReduce(p);
    return this;
  }

  /**
   * Returns an unmodifiable view of the list of {@link MapReduceDriver} instances
   * that make up the pipeline.
   *
   * @return An unmodifiable list of {@link MapReduceDriver}s in the pipeline.
   */
  public List<MapReduceDriver> getMapReducePipeline() {
    return Collections.unmodifiableList(mapReducePipeline);
  }

  /**
   * Adds an input key/value pair to be sent to the first Mapper in the pipeline.
   *
   * @param key The input key.
   * @param val The input value.
   */
  public void addInput(K1 key, V1 val) {
    inputList.add(new Pair<K1, V1>(key, val));
  }

  /**
   * Adds an input key/value pair to be sent to the first Mapper in the pipeline
   * and returns the driver instance for fluent programming.
   *
   * @param key The input key.
   * @param val The input value.
   * @return this {@link PipelineMapReduceDriver} instance.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(K1 key, V1 val) {
    addInput(key, val);
    return this;
  }

  /**
   * Adds an input {@link Pair} to be sent to the first Mapper in the pipeline.
   *
   * @param input The (key, value) {@link Pair} to add to the input list.
   * @throws IllegalArgumentException if the input Pair is null.
   */
  public void addInput(Pair<K1, V1> input) {
    if (null == input) {
      throw new IllegalArgumentException("Null input in addInput()");
    }
    inputList.add(input);
  }

  /**
   * Adds an input {@link Pair} to be sent to the first Mapper in the pipeline
   * and returns the driver instance for fluent programming.
   *
   * @param input The (key, value) {@link Pair} to add.
   * @return this {@link PipelineMapReduceDriver} instance.
   * @throws IllegalArgumentException if the input Pair is null.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(
      Pair<K1, V1> input) {
    addInput(input);
    return this;
  }

  /**
   * Adds an expected output (key, value) {@link Pair} from the final Reducer
   * in the pipeline.
   *
   * @param outputRecord The (key, value) {@link Pair} to expect as output.
   * @throws IllegalArgumentException if the outputRecord is null.
   */
  public void addOutput(Pair<K2, V2> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Adds an expected output (key, value) {@link Pair} from the final Reducer
   * in the pipeline and returns the driver instance for fluent programming.
   *
   * @param outputRecord The (key, value) {@link Pair} to expect as output.
   * @return this {@link PipelineMapReduceDriver} instance.
   * @throws IllegalArgumentException if the outputRecord is null.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutput(
          Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Adds an expected output key/value pair from the final Reducer in the pipeline.
   *
   * @param key The expected output key.
   * @param val The expected output value.
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Adds an expected output key/value pair from the final Reducer in the pipeline
   * and returns the driver instance for fluent programming.
   *
   * @param key The expected output key.
   * @param val The expected output value.
   * @return this {@link PipelineMapReduceDriver} instance.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutput(K2 key, V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Adds an input record from a tab-delimited string of the form "key \t val".
   * This method forces the initial input key and value types of the pipeline
   * (K1, V1) to {@link Text}.
   * <p>
   * Note: This method involves a type cast to {@code Pair<K1, V1>}. This is
   * inherently not type-safe if K1 and V1 are not Text. This method is provided
   * for convenience in simple test cases where Text input is common.
   * If your pipeline's initial input types are not Text, use the
   * {@link #addInput(Object, Object)} or {@link #addInput(Pair)} methods instead.
   * </p>
   *
   * @param input A string of the form "key \t val". Leading/trailing whitespace is trimmed.
   * @throws IllegalArgumentException if input is null or cannot be parsed.
   */
  public void addInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input given to setInput");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // This cast is not type-safe. It assumes K1 and V1 are Text.
        // This is a known limitation for convenience in common Text-based scenarios.
        // For other types, use addInput(K1, V1) or addInput(Pair<K1,V1>).
        addInput((Pair<K1, V1>) inputPair);
      } else {
        throw new IllegalArgumentException("Could not parse input pair in addInput");
      }
    }
  }

  /**
   * Adds an input record from a tab-delimited string and returns the driver
   * instance for fluent programming. Forces initial input types (K1, V1) to {@link Text}.
   * See {@link #addInputFromString(String)} for details on type safety.
   *
   * @param input A string of the form "key \t val".
   * @return this {@link PipelineMapReduceDriver} instance.
   * @throws IllegalArgumentException if input is null or cannot be parsed.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInputFromString(String input) {
    addInputFromString(input);
    return this;
  }

  /**
   * Adds an expected output record from a tab-delimited string of the form "key \t val".
   * This method forces the final output key and value types of the pipeline
   * (K2, V2) to {@link Text}.
   * <p>
   * Note: This method involves a type cast to {@code Pair<K2, V2>}. This is
   * inherently not type-safe if K2 and V2 are not Text. This method is provided
   * for convenience in simple test cases where Text output is common.
   * If your pipeline's final output types are not Text, use the
   * {@link #addOutput(Object, Object)} or {@link #addOutput(Pair)} methods instead.
   * </p>
   *
   * @param output A string of the form "key \t val". Leading/trailing whitespace is trimmed.
   * @throws IllegalArgumentException if output is null or cannot be parsed.
   */
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input given to setOutput");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // This cast is not type-safe. It assumes K2 and V2 are Text.
        // This is a known limitation for convenience in common Text-based scenarios.
        // For other types, use addOutput(K2, V2) or addOutput(Pair<K2,V2>).
        addOutput((Pair<K2, V2>) outputPair);
      } else {
        throw new IllegalArgumentException(
            "Could not parse output pair in setOutput");
      }
    }
  }

  /**
   * Adds an expected output record from a tab-delimited string and returns the driver
   * instance for fluent programming. Forces final output types (K2, V2) to {@link Text}.
   * See {@link #addOutputFromString(String)} for details on type safety.
   *
   * @param output A string of the form "key \t val".
   * @return this {@link PipelineMapReduceDriver} instance.
   * @throws IllegalArgumentException if output is null or cannot be parsed.
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutputFromString(String output) {
    addOutputFromString(output);
    return this;
  }

  /**
   * Executes the entire pipeline of MapReduce jobs.
   * The initial inputs are fed into the first {@link MapReduceDriver}. The outputs
   * of that driver then become the inputs for the next driver in the pipeline,
   * and so on.
   *
   * @return A list of (key, value) {@link Pair}s representing the final output
   *         from the last {@link MapReduceDriver} in the pipeline.
   * @throws IOException if any MapReduce job in the pipeline encounters an I/O error.
   */
  public List<Pair<K2, V2>> run() throws IOException {
    // 'currentInputs' starts as the user-provided inputs for the first MR job.
    List currentInputs = this.inputList;

    if (mapReducePipeline.isEmpty()) {
      LOG.warn("No Mapper or Reducer instances in pipeline; this is a trivial test.");
    }

    if (currentInputs.isEmpty() && !mapReducePipeline.isEmpty()) {
      LOG.warn("No inputs configured to send to MapReduce pipeline; this is a trivial test unless the first job generates data.");
    }

    // Iterate through each MapReduce job in the pipeline
    for (MapReduceDriver driver : mapReducePipeline) {
      // Propagate counters; each job contributes to the same set of counters.
      driver.setCounters(getCounters());

      // Clear any inputs from a previous iteration of *this* driver instance
      // if it were (incorrectly) reused in the pipeline list.
      // This also handles the case where a driver might have its own input list.
      driver.resetInput();


      // Add the inputs from the user (for the first job) or
      // from the output of the previous stage of the pipeline.
      for (Object input : currentInputs) {
        driver.addInput((Pair) input);
      }

      // Run the current MapReduce "job".
      // The output of this job becomes the input to the next job.
      currentInputs = driver.run();
    }

    // After all jobs have run, 'currentInputs' now holds the final outputs
    // from the last job in the pipeline.
    // This cast is potentially unsafe if the pipeline's K2, V2 types do not
    // match the actual output types of the last driver. The generic types
    // of PipelineMapReduceDriver should align with the last driver's output.
    return (List<Pair<K2, V2>>) currentInputs;
  }

  /**
   * Executes the pipeline and validates the final output against the expected outputs.
   * This is the primary method for running a test case.
   *
   * @throws RuntimeException if an {@link IOException} occurs during execution
   *                          or if the actual output does not match the expected output.
   */
  @Override
  public void runTest() throws RuntimeException {
    List<Pair<K2, V2>> outputs = null;

    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException(ioe); // Preserve the original exception
    }
  }
}
