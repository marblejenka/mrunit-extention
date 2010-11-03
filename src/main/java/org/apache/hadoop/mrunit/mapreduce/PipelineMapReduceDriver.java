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
 * Pipeline Driver for new api.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PipelineMapReduceDriver<K1, V1, K2, V2>
    extends TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(PipelineMapReduceDriver.class);

//  private List<Pair<Mapper, Reducer>> mapReducePipeline;
  private List<MapReduceDriver> mapReducePipeline; 
  private List<Pair<K1, V1>> inputList;
  private Counters counters;
  
  public PipelineMapReduceDriver(final MapReduceDriver... mapReducePipeline) {
    this.mapReducePipeline.addAll(Arrays.asList(mapReducePipeline));
    this.inputList = new ArrayList<Pair<K1, V1>>();
    this.counters = new Counters();
  }

  public PipelineMapReduceDriver() {
    this.mapReducePipeline = new ArrayList<MapReduceDriver>();
    this.inputList = new ArrayList<Pair<K1, V1>>();
    this.counters = new Counters();
  }

  /** @return the counters used in this test */
  public Counters getCounters() {
    return counters;
  }

  /** Sets the counters object to use for this test.
   * @param ctrs The counters object to use.
   */
  public void setCounters(final Counters ctrs) {
    this.counters = ctrs;
  }

  /** Sets the counters to use and returns self for fluent style */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }


  /** Add a Mapper and Reducer instance to the pipeline to use with this test driver
   * @param m The Mapper instance to add to the pipeline
   * @param r The Reducer instance to add to the pipeline
   */
  public void addMapReduce(Mapper m, Reducer r) {
    addMapReduce(new MapReduceDriver(m, r));
  }

  /** Add a Mapper and Reducer instance to the pipeline to use with this test driver
   * @param p The Mapper and Reducer instances to add to the pipeline
   */
  public void addMapReduce(MapReduceDriver driver) {
    this.mapReducePipeline.add(driver);
  }

  /** Add a Mapper and Reducer instance to the pipeline to use with this test driver
   * using fluent style
   * @param m The Mapper instance to use
   * @param r The Reducer instance to use
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(Mapper m, Reducer r) {
    addMapReduce(m, r);
    return this;
  }

  /** Add a Mapper and Reducer instance to the pipeline to use with this test driver
   * using fluent style
   * @param p The Mapper and Reducer instances to add to the pipeline
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(MapReduceDriver p) {
    addMapReduce(p);
    return this;
  }

  /**
   * @return A copy of the list of Mapper and Reducer objects under test
   */
  public List<MapReduceDriver> getMapReducePipeline() {
    return Collections.unmodifiableList(mapReducePipeline);
  }

  /**
   * Adds an input to send to the mapper
   * @param key
   * @param val
   */
  public void addInput(K1 key, V1 val) {
    inputList.add(new Pair<K1, V1>(key, val));
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * @param key
   * @param val
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(K1 key, V1 val) {
    addInput(key, val);
    return this;
  }

  /**
   * Adds an input to send to the Mapper
   * @param input The (k, v) pair to add to the input list.
   */
  public void addInput(Pair<K1, V1> input) {
    if (null == input) {
      throw new IllegalArgumentException("Null input in addInput()");
    }

    inputList.add(input);
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * @param input The (k, v) pair to add
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(
      Pair<K1, V1> input) {
    addInput(input);
    return this;
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * @param outputRecord The (k, v) pair to add
   */
  public void addOutput(Pair<K2, V2> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * @param outputRecord
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutput(
          Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Adds a (k, v) pair we expect as output from the Reducer
   * @param key
   * @param val
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Functions like addOutput() but returns self for fluent programming style
   * @param key
   * @param val
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutput(K2 key, V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Expects an input of the form "key \t val"
   * Forces the Mapper input types to Text.
   * @param input A string of the form "key \t val". Trims any whitespace.
   */
  public void addInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input given to setInput");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // I know this is not type-safe, but I don't
        // know a better way to do this.
        addInput((Pair<K1, V1>) inputPair);
      } else {
        throw new IllegalArgumentException("Could not parse input pair in addInput");
      }
    }
  }

  /**
   * Identical to addInputFromString, but with a fluent programming style
   * @param input A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInputFromString(String input) {
    addInputFromString(input);
    return this;
  }

  /**
   * Expects an input of the form "key \t val"
   * Forces the Reducer output types to Text.
   * @param output A string of the form "key \t val". Trims any whitespace.
   */
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input given to setOutput");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // I know this is not type-safe,
        // but I don't know a better way to do this.
        addOutput((Pair<K2, V2>) outputPair);
      } else {
        throw new IllegalArgumentException(
            "Could not parse output pair in setOutput");
      }
    }
  }

  /**
   * Identical to addOutputFromString, but with a fluent programming style
   * @param output A string of the form "key \t val". Trims any whitespace.
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withOutputFromString(String output) {
    addOutputFromString(output);
    return this;
  }

  public List<Pair<K2, V2>> run() throws IOException {
    // inputs starts with the user-provided inputs.
    List inputs = this.inputList;

    if (mapReducePipeline.size() == 0) {
      LOG.warn("No Mapper or Reducer instances in pipeline; this is a trivial test.");
    }

    if (inputs.size() == 0) {
      LOG.warn("No inputs configured to send to MapReduce pipeline; this is a trivial test.");
    }

    for (MapReduceDriver driver : mapReducePipeline) {
      driver.setCounters(getCounters());

      // Add the inputs from the user, or from the previous stage of the pipeline.
      for (Object input : inputs) {
        driver.addInput((Pair) input);
      }

      // Run the MapReduce "job". The output of this job becomes
      // the input to the next job.
      inputs = driver.run();
    }

    // The last list of values stored in "inputs" is actually the outputs.
    // Unfortunately, due to the variable-length list of MR passes the user 
    // can test, this is not type-safe.
    return (List<Pair<K2, V2>>) inputs;
  }

  @Override
  public void runTest() throws RuntimeException {
    List<Pair<K2, V2>> outputs = null;

    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException();
    }
  }
}
