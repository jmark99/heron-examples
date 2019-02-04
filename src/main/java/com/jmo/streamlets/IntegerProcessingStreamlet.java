package com.jmo.streamlets;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

public class IntegerProcessingStreamlet {

  private static final Logger LOG = Logger.getLogger(IntegerProcessingStreamlet.class.getName());

  private static String topologyName;
  private static int msgTimeout = 30;
  private static int delay = 1; // milisecond delay between emitting of tuples.
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main( String[] args ) throws Exception {

    LOG.info(">>> topologyName: " + topologyName);
    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    IntegerProcessingStreamlet intProcessor = new IntegerProcessingStreamlet();
    intProcessor.runStreamlet(topologyName);
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> IntegerProcessingStreamlet...");

    Builder builder = Builder.newBuilder();

    integerProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private void integerProcessingGraph(Builder builder) {
    Streamlet<Integer> zeroes = builder.newSource(() -> {
      StreamletUtils.sleep(1000);
      return 0;});

    if (!addDelay) {
      builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
          .setName("random-ints")
          .map(i -> i * 10)
          .setName("multi-ten")
          .union(zeroes)
          .setName("unify-streams")
          .filter(i -> i != 20)
          .setName("remove-twenties")
          .log();
    } else {
      builder.newSource(() -> {
        StreamletUtils.sleep(delay);
        return ThreadLocalRandom.current()
            .nextInt(1, 11); })
          .setName("random-ints")
          .map(i -> i * 10)
          .setName("multi-ten")
          .union(zeroes)
          .setName("unify-streams")
          .filter(i -> i != 20)
          .setName("remove-twenties")
          .log();
    }
  }
}
