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
 * Filter operations retain elements in a streamlet, while potentially excluding some or all
 * elements, on the basis of a provided filtering function. In this example, a source streamlet
 * consisting of random integers is modified by a filter operation that remove all streamlet
 * elements that are greater than 7.
 * <p>
 * This example is based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleFilterStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleFilterStreamlet.class.getName());

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

    SimpleFilterStreamlet streamletInstance = new SimpleFilterStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimpleFilterStreamlet...");

    Builder builder = Builder.newBuilder();
    createFilterProcessingGraph(builder);

    Config config = Config.newBuilder().setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM).setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics).setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private void createFilterProcessingGraph(Builder builder) {
    Streamlet<Integer> integers = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current()
          .nextInt(1, 11); });

    integers
        .setName("random-int-source")
        .filter((i) -> i < 7)
        .setName("discard-seven-and-up")
        .log();
  }
}