package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class ComplexSourceStreamlet {

  private static final Logger LOG = Logger.getLogger(ComplexSourceStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public ComplexSourceStreamlet() {
  }

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:   " + addDelay);
    LOG.info(">>> msDelay:    " + msDelay);
    LOG.info(">>> nsDelay:    " + nsDelay);
    LOG.info(">>> msgTimeout: " + msgTimeout);
    LOG.info(">>> semantics:  " + semantics);

    ComplexSourceStreamlet complexTopology = new ComplexSourceStreamlet();
    complexTopology.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run ComplexSourceStreamlet...");

    Builder builder = Builder.newBuilder();
    createComplexSourcePRocessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false).build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private static class IntegerSource implements Source<Integer> {

    private static final long serialVersionUID = 7270252642157243930L;
    Random rnd = new Random();
    List<Integer> intList;

    IntegerSource() {
      intList = new ArrayList<>();
    }

    /**
     * The setup functions defines the instantiation logic for the source.
     */
    public void setup(Context context) {
    }

    /**
     * The get function defines how elements for the source streamlet are
     * gotten.
     */
    public Collection<Integer> get() {
      intList.clear();
      int i = rnd.nextInt(25) + 1;
      intList.add(i + 1);
      intList.add(i + 2);
      intList.add(i + 3);
      StreamletUtils.sleep(msDelay, nsDelay);
      return intList;
    }

    public void cleanup() {
    }
  }

  private static class ComplexIntegerSink<T> implements Sink<T> {

    private static final long serialVersionUID = -2565224820596857979L;

    ComplexIntegerSink() {
    }

    /**
     * The setup function is called before the sink is used. Any complex
     * instantiation logic for the sink should go here.
     */
    public void setup(Context context) {
    }

    /**
     * The put function defines how each incoming streamlet element is
     * actually processed. In this case, each incoming element is converted
     * to a byte array and written to the temporary file (successful writes
     * are also logged). Any exceptions are converted to RuntimeExceptions,
     * which will effectively kill the topology.
     */
    public void put(T element) {
      LOG.info(">>>> element: " + element);
    }

    /**
     * Any cleanup logic for the sink can be applied here.
     */
    public void cleanup() {
    }
  }

  private void createComplexSourcePRocessingGraph(Builder builder) {
    Source<Integer> integerSource = new IntegerSource();

    builder.newSource(integerSource)
        .setName("integer-source")
        .map(i -> i * 100)
        .toSink(new ComplexIntegerSink<>());
  }
}
