package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

public class SimpleMapStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleMapStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int delay = 1; // milisecond delay between emitting of tuples.
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleMapStreamlet streamletInstance = new SimpleMapStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> runStreamlet: " + this.getClass().getSimpleName());

    Builder builder = Builder.newBuilder();
    createSimpleMapProcessingGraph(builder);

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

  private void createSimpleMapProcessingGraph(Builder builder) {

    if (!addDelay) {
      builder.newSource(() -> 1)
          .setName("ones")
          .map(i -> i + 12)
          .setName("add-twelve")
          .log();
    } else {
      builder.newSource(() -> {
        StreamletUtils.sleepnano(delay);
        return 1;
      })
          .setName("ones")
          .map(i -> i + 12)
          .setName("add-twelve")
          .log();
    }
  }
}