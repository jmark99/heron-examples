package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * Union operations combine two streamlets of the same type into a single streamlet without
 * modifying the elements.
 * <p>
 * In this example,  one streamlet is an endless series of "ooh"s while the other is an endless
 * series of "aah"s. The union operation combines them into a single streamlet of alternating
 * "ooh"s and "aah"s.
 * <p>
 * This example is based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleUnionStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleUnionStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleUnionStreamlet streamletInstance = new SimpleUnionStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    Builder builder = Builder.newBuilder();
    createUnionProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private void createUnionProcessingGraph(Builder builder) {

    Streamlet<String> oohs = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "ooh";
    });

    Streamlet<String> aahs = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "aah";
    });

    oohs.union(aahs)
        .log();
  }

}
