package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

/**
 * Map Operations create a new streamlet by applying the supplied mapping function to each element
 * in the original streamlet.
 * <p>
 * This simple example, based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>, takes an input
 * stream of all ones and creates a new streamlet with 12 added to each value in the original
 * streamlet.
 */
public class SimpleMapStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleMapStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> msDelay:      " + msDelay);
    LOG.info(">>> nsDelay:      " + nsDelay);
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

    Streamlet<Integer> onesSource = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return 1;
    });

    onesSource
        .setName("ones-supplier")
        .map(i -> i + 12)
        .setName("add-twelve")
        .log();

  }
}